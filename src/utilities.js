/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 ********************************************************************************/

import { createClient } from "@libsql/client";
import {
	dotNotationToObject,
	queryData,
	searchData,
	sortData,
	isValidDate
} from "@cocreate/utils";

const organizations = {};

/**
 * Dynamically resolves or caches a long-lived libSQL Database Client.
 * Supports standard local SQLite files ("file:local.db"), in-memory paths (":memory:"),
 * and remote Turso connections ("libsql://...") with cloud tokens.
 */
async function dbClient(data) {
	if (data.storageUrl) {
		if (!organizations[data.organization_id]) {
			organizations[data.organization_id] = {};
		}
		try {
			if (!organizations[data.organization_id][data.storageUrl]) {
				const connectionPromise = (async () => {
					let config = { url: data.storageUrl };

					// If an authentication token is passed inside dynamic payloads for cloud/Turso targets
					if (data.authToken || data.auth_token) {
						config.authToken = data.authToken || data.auth_token;
					}

					const client = createClient(config);

					// Verify active driver connectivity safely
					await client.execute("SELECT 1;");
					return client;
				})();

				organizations[data.organization_id][data.storageUrl] = connectionPromise;
			}

			return await organizations[data.organization_id][data.storageUrl];
		} catch (error) {
			console.error(
				`${data.organization_id}: Connection failed using @libsql/client`
			);
			errorHandler(data, error);
			return { status: false };
		}
	}

	errorHandler(data, "missing StorageUrl");
	return;
}

/**
 * Global Event Listener: orgDeleted
 * Gracefully terminates active libSQL database connections and clears the local pool cache.
 */
process.on("orgDeleted", (organization_id) => {
	const orgPools = organizations[organization_id];
	if (orgPools) {
		console.log(`[libSQL Utilities] Cleanup Scheduled: Initiating 5-second grace period for Org: ${organization_id}`);
		setTimeout(async () => {
			const currentPools = organizations[organization_id];
			if (!currentPools) return;

			console.log(`[libSQL Utilities] Cleanup Executing: Closing connection clients for Org: ${organization_id}`);
			const dbList = Object.values(currentPools);

			for (const dbPromise of dbList) {
				try {
					const db = await dbPromise;
					if (db && typeof db.close === "function") {
						db.close();
					}
				} catch (err) {
					console.error(`[libSQL Utilities] Error closing connection for Org ${organization_id}:`, err);
				}
			}

			delete organizations[organization_id];
			console.log(`[libSQL Utilities] Memory Cleared: Registry freed for Org: ${organization_id}`);
		}, 5000);
	}
});

/**
 * Safe transaction helper to update unstructured payloads with MongoDB behavior inside libSQL.
 */
async function executeUpdateTransaction(db, arrayName, docId, rawUpdateInput, requestData) {
	try {
		const selectSql = `SELECT * FROM "${arrayName}" WHERE _id = ? AND organization_id = ?;`;

		// Run isolation transaction
		const tx = await db.transaction("deferred");

		const currentRecordRaw = await tx.execute({ sql: selectSql, args: [docId, requestData.organization_id] });
		let currentRecord = currentRecordRaw.rows.length > 0 ? parseSqlRow(currentRecordRaw.rows[0]) : null;

		if (!currentRecord) {
			if (requestData.upsert || rawUpdateInput.upsert || rawUpdateInput.$upsert) {
				currentRecord = { _id: docId, organization_id: requestData.organization_id };
			} else {
				await tx.rollback();
				return null;
			}
		}

		const modified = {
			on: new Date(requestData.timeStamp),
			by: requestData.user_id || requestData.clientId
		};

		const updatedPayload = applyMongoUpdate(currentRecord, rawUpdateInput);

		const savePayload = { ...updatedPayload };
		delete savePayload._id;
		delete savePayload.organization_id;
		delete savePayload.created;
		delete savePayload.modified;

		const updateSql = `
			UPDATE "${arrayName}"
			SET data = ?,
			    modified = ?
			WHERE _id = ? AND organization_id = ?;
		`;
		const updateParams = [JSON.stringify(savePayload), JSON.stringify(modified), docId, requestData.organization_id];

		process.emit("usage", {
			type: "egress",
			data: { sql: updateSql, updateParams },
			organization_id: requestData.organization_id
		});

		await tx.execute({ sql: updateSql, args: updateParams });
		await tx.commit();

		const finalRecordRaw = await db.execute({ sql: selectSql, args: [docId, requestData.organization_id] });
		return parseSqlRow(finalRecordRaw.rows[0]);
	} catch (error) {
		throw error;
	}
}

/**
 * Applies MongoDB update operators recursively to maintain unified schema manipulation behaviors.
 */
function applyMongoUpdate(record, updatePayload) {
	let target = { ...record };

	Object.keys(updatePayload).forEach((rawKey) => {
		if (rawKey.startsWith("$")) return;
		let cleanKey = rawKey.replace(/\[(\d+)\]/g, ".$1");
		setNestedValue(target, cleanKey, updatePayload[rawKey]);
	});

	if (updatePayload.$set) {
		Object.keys(updatePayload.$set).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			setNestedValue(target, cleanKey, updatePayload.$set[key]);
		});
	}

	if (updatePayload.$unset) {
		Object.keys(updatePayload.$unset).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			deleteNestedValue(target, cleanKey);
		});
	}

	if (updatePayload.$inc) {
		Object.keys(updatePayload.$inc).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let currentVal = getNestedValue(target, cleanKey) || 0;
			setNestedValue(target, cleanKey, Number(currentVal) + Number(updatePayload.$inc[key]));
		});
	}

	if (updatePayload.$push) {
		Object.keys(updatePayload.$push).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let arr = getNestedValue(target, cleanKey);
			if (!Array.isArray(arr)) arr = [];

			const pushValue = updatePayload.$push[key];
			if (pushValue && pushValue.$each) {
				let valuesToPush = pushValue.$each;
				let position = typeof pushValue.$position === "number" ? pushValue.$position : arr.length;
				arr.splice(position, 0, ...valuesToPush);
			} else {
				arr.push(pushValue);
			}
			setNestedValue(target, cleanKey, arr);
		});
	}

	if (updatePayload.$pull) {
		Object.keys(updatePayload.$pull).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let arr = getNestedValue(target, cleanKey);
			if (Array.isArray(arr)) {
				const filterVal = updatePayload.$pull[key];
				arr = arr.filter(item => {
					if (typeof filterVal === "object" && filterVal !== null) {
						return !queryData(item, filterVal);
					}
					return item !== filterVal;
				});
				setNestedValue(target, cleanKey, arr);
			}
		});
	}

	if (updatePayload.$addToSet) {
		Object.keys(updatePayload.$addToSet).forEach((key) => {
			let cleanKey = key.replace(/\[(\d+)\]/g, ".$1");
			let arr = getNestedValue(target, cleanKey);
			if (!Array.isArray(arr)) arr = [];

			const addValue = updatePayload.$addToSet[key];
			if (addValue && addValue.$each) {
				addValue.$each.forEach(item => {
					if (!arr.includes(item)) arr.push(item);
				});
			} else {
				if (!arr.includes(addValue)) arr.push(addValue);
			}
			setNestedValue(target, cleanKey, arr);
		});
	}

	return target;
}

/**
 * Translates standard MongoDB JSON filters into native SQLite query filters.
 */
function compileFilterToSQL(filter, organizationId) {
	let clauses = ["organization_id = ?"];
	let params = [organizationId];

	if (filter && filter.query) {
		const parseConditionGroup = (queryObj, isOrJoin = false) => {
			let localClauses = [];

			if (queryObj.$or && Array.isArray(queryObj.$or)) {
				let orClauses = queryObj.$or.map(subQuery => parseConditionGroup(subQuery)).filter(c => c.length);
				if (orClauses.length) {
					localClauses.push("(" + orClauses.join(" OR ") + ")");
				}
			}
			if (queryObj.$and && Array.isArray(queryObj.$and)) {
				let andClauses = queryObj.$and.map(subQuery => parseConditionGroup(subQuery)).filter(c => c.length);
				if (andClauses.length) {
					localClauses.push("(" + andClauses.join(" AND ") + ")");
				}
			}

			for (let key in queryObj) {
				if (["$or", "$and"].includes(key)) continue;

				let value = queryObj[key];
				const targetField = ["_id", "organization_id"].includes(key)
					? `"${key}"`
					: `json_extract(data, '$.${key}')`;

				if (value && typeof value === "object" && !Array.isArray(value)) {
					for (let op in value) {
						let operand = value[op];
						params.push(operand);

						if (op === "$gt") localClauses.push(`${targetField} > ?`);
						else if (op === "$gte") localClauses.push(`${targetField} >= ?`);
						else if (op === "$lt") localClauses.push(`${targetField} < ?`);
						else if (op === "$lte") localClauses.push(`${targetField} <= ?`);
						else if (op === "$ne") localClauses.push(`${targetField} != ?`);
						else if (op === "$in") {
							params.pop(); // Remove single item
							const placeholders = operand.map(() => "?").join(", ");
							operand.forEach(o => params.push(o));
							localClauses.push(`${targetField} IN (${placeholders})`);
						}
					}
				} else {
					params.push(value);
					localClauses.push(`${targetField} = ?`);
				}
			}

			return localClauses.join(isOrJoin ? " OR " : " AND ");
		};

		const processedQuery = parseConditionGroup(filter.query);
		if (processedQuery) {
			clauses.push(processedQuery);
		}
	}

	return {
		sqlWhere: clauses.length > 0 ? "WHERE " + clauses.join(" AND ") : "",
		params
	};
}

/**
 * Resolves SQLite columns and rows back into unified JSON format.
 */
function parseSqlRow(row) {
	if (!row) return null;
	const recordData = typeof row.data === "string" ? JSON.parse(row.data) : (row.data || {});
	return {
		_id: row._id,
		organization_id: row.organization_id,
		created: typeof row.created === "string" ? JSON.parse(row.created) : row.created,
		modified: typeof row.modified === "string" ? JSON.parse(row.modified) : row.modified,
		...recordData
	};
}

function replaceArray(data = {}) {
	let object = {};
	Object.keys(data).forEach((key) => {
		object[key.replace(/\[(\d+)\]/g, ".$1")] = data[key];
	});
	return object;
}

function getNestedValue(obj, path) {
	return path.split(".").reduce((acc, part) => acc && acc[part], obj);
}

function setNestedValue(obj, path, value) {
	const parts = path.split(".");
	const last = parts.pop();
	const target = parts.reduce((acc, part) => acc[part] = acc[part] || {}, obj);
	target[last] = value;
}

function deleteNestedValue(obj, path) {
	const parts = path.split(".");
	const last = parts.pop();
	const target = parts.reduce((acc, part) => acc && acc[part], obj);
	if (target) delete target[last];
}

/**
 * Syncs the output response data arrays back cleanly.
 */
function createData(data, array, type) {
	if (data[type] && data[type][0] && data[type][0].isFilter === "isEmptyObjectFilter") {
		data[type].shift();
		data.isFilter = true;
	}

	let key = type !== "object" ? "name" : "_id";

	if (!Array.isArray(data[type])) {
		console.log("data[type] is not an array", type);
	} else {
		for (let i = 0; i < array.length; i++) {
			const matchIndex = data[type].findIndex((item) => item[key] === array[i][key]);
			if (matchIndex !== -1) {
				for (let $type of ["$storage", "$database", "$array"]) {
					if (!data[type][matchIndex][$type]) data[type][matchIndex][$type] = [];
					if (!Array.isArray(data[type][matchIndex][$type])) {
						data[type][matchIndex][$type] = [data[type][matchIndex][$type]];
					}
					if (!data[type][matchIndex][$type].includes(array[i][$type])) {
						data[type][matchIndex][$type].push(array[i][$type]);
					}
					delete array[i][$type];
				}
				data[type][matchIndex] = { ...data[type][matchIndex], ...array[i] };
			} else {
				data[type].push(array[i]);
			}
		}
	}
	return data;
}

function errorHandler(data, error, database, array) {
	let errorMessage = typeof error === "object" && error.message ? error.message : error;
	let errorObject = {
		message: errorMessage,
		storage: "sqlite"
	};

	if (database) errorObject.database = database;
	if (array) errorObject.array = array;

	if (Array.isArray(data.error)) {
		data.error.push(errorObject);
	} else {
		data.error = [errorObject];
	}
}

export {
    dbClient,
    createData,
    errorHandler
};