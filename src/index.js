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
 * Universal router routing request methods safely.
 */
export function send(data) {
	let [type, method] = data.method.split(".");
	if (type === "database") return database(method, data);
	if (type === "array") return array(method, data);
	if (type === "object") return object(method, data);
}

/**
 * "database" operations mapped to SQLite Files (emulated catalogs).
 */
function database(method, data) {
	const storageName = data.storageName || data.provider || "sqlite";
	return new Promise(
		async (resolve, reject) => {
			let type = "database";
			let databaseArray = [];

			try {
				const db = await dbClient(data);
				if (!db || db.status === false) return resolve(data);

				if (method === "read") {
					const sql = "PRAGMA database_list;";
					process.emit("usage", {
						type: "egress",
						data: { sql },
						organization_id: data.organization_id
					});

					const res = await db.execute(sql);
					process.emit("usage", {
						type: "ingress",
						data: res.rows,
						organization_id: data.organization_id
					});

					for (let row of res.rows) {
						// libSQL ResultSet rows support standard property lookup
						const dbObj = { name: row.file ? row.file.split("/").pop() : "memory" };
						if (data.$filter && data.$filter.query) {
							let isFilter = queryData(dbObj, data.$filter.query);
							if (isFilter) {
								databaseArray.push({
									database: dbObj,
									storage: storageName
								});
							}
						} else {
							databaseArray.push({
								database: dbObj,
								storage: storageName
							});
						}
					}
					resolve(createData(data, databaseArray, type));
				}
				if (method === "delete") {
					// Drop all non-system tables inside the database context safely
					const tables = await db.execute("SELECT name FROM sqlite_master WHERE type='table';");
					for (let t of tables.rows) {
						if (!t.name.startsWith("sqlite_")) {
							const sql = `DROP TABLE IF EXISTS "${t.name}";`;
							process.emit("usage", {
								type: "egress",
								data: { sql },
								organization_id: data.organization_id
							});
							await db.execute(sql);
						}
					}
					resolve({ status: true });
				}
			} catch (error) {
				errorHandler(data, error);
				console.log(method, "error", error);
				resolve(data);
			}
		},
		(error) => {
			errorHandler(data, error);
		}
	);
}

/**
 * "array" operations mapped to SQL Tables.
 */
function array(method, data) {
	const storageName = data.storageName || data.provider || "sqlite";
	return new Promise(
		async (resolve, reject) => {
			let type = "array";
			let arrayArray = [];

			try {
				const db = await dbClient(data);
				if (!db || db.status === false) return resolve(data);

				if (data.request) data.array = data.request;

				let databases = data.database;
				if (!Array.isArray(databases)) databases = [databases];

				let databasesLength = databases.length;
				for (let database of databases) {
					if (method === "read") {
						const sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';";
						process.emit("usage", {
							type: "egress",
							data: { sql },
							organization_id: data.organization_id
						});

						const res = await db.execute(sql);
						process.emit("usage", {
							type: "ingress",
							data: res.rows,
							organization_id: data.organization_id
						});

						for (let row of res.rows) {
							const tableObj = { name: row.name };
							if (data.$filter && data.$filter.query) {
								let isFilter = queryData(tableObj, data.$filter.query);
								if (isFilter) {
									arrayArray.push({
										name: row.name,
										database,
										storage: storageName
									});
								}
							} else {
								arrayArray.push({
									name: row.name,
									database,
									storage: storageName
								});
							}
						}

						databasesLength -= 1;
						if (!databasesLength) {
							data = createData(data, arrayArray, type);
							resolve(data);
						}
					} else {
						let arrays = data.array;
						if (method === "update") arrays = Object.entries(data.array);
						if (!Array.isArray(arrays)) arrays = [arrays];

						let arraysLength = arrays.length;
						for (let array of arrays) {
							if (method === "create") {
								const sql = `
									CREATE TABLE IF NOT EXISTS "${array}" (
										_id TEXT PRIMARY KEY,
										organization_id TEXT,
										data TEXT DEFAULT '{}',
										created TEXT,
										modified TEXT
									);
								`;
								process.emit("usage", {
									type: "egress",
									data: { sql },
									organization_id: data.organization_id
								});

								await db.execute(sql);
								arrayArray.push({
									name: array,
									database,
									storage: storageName
								});

								arraysLength -= 1;
								if (!arraysLength) databasesLength -= 1;
								if (!databasesLength && !arraysLength) {
									data = createData(data, arrayArray, type);
									resolve(data);
								}
							} else {
								if (method === "update") {
									let [oldName, newName] = array;
									const sql = `ALTER TABLE "${oldName}" RENAME TO "${newName}";`;
									process.emit("usage", {
										type: "egress",
										data: { sql },
										organization_id: data.organization_id
									});

									await db.execute(sql);
									arrayArray.push({
										name: newName,
										oldName: oldName,
										database,
										storage: storageName
									});

									arraysLength -= 1;
									if (!arraysLength) databasesLength -= 1;
									if (!databasesLength && !arraysLength) {
										data = createData(data, arrayArray, type);
										resolve(data);
									}
								}

								if (method === "delete") {
									const sql = `DROP TABLE IF EXISTS "${array}";`;
									process.emit("usage", {
										type: "egress",
										data: { sql },
										organization_id: data.organization_id
									});

									await db.execute(sql);
									arrayArray.push({
										name: array,
										database,
										storage: storageName
									});

									arraysLength -= 1;
									if (!arraysLength) databasesLength -= 1;
									if (!databasesLength && !arraysLength) {
										data = createData(data, arrayArray, type);
										resolve(data);
									}
								}
							}
						}
					}
				}
			} catch (error) {
				errorHandler(data, error);
				console.log(method, "error", error);
				resolve(data);
			}
		},
		(error) => {
			errorHandler(data, error);
		}
	);
}

/**
 * "object" operations mapped to SQL Rows.
 */
function object(method, data) {
	const storageName = data.storageName || data.provider || "sqlite";
	return new Promise(
		async (resolve, reject) => {
			try {
				const db = await dbClient(data);
				if (!db || db.status === false) return resolve(data);

				let type = "object";
				let documents = [];

				if (!data["timeStamp"]) {
					data["timeStamp"] = new Date().toISOString();
				}

				let databases = data.database;
				if (!Array.isArray(databases)) databases = [databases];

				for (let database of databases) {
					let arrays = data.array;
					if (!Array.isArray(arrays)) arrays = [arrays];

					for (let array of arrays) {
						const reference = {
							$storage: storageName,
							$database: database,
							$array: array
						};

						if (!data[type]) data[type] = [];
						else if (typeof data[type] === "string")
							data[type] = [{ _id: data[type] }];
						else if (!Array.isArray(data[type]))
							data[type] = [data[type]];

						let isFilter = !!data.$filter;
						if ((isFilter && !data[type].length) || data.isFilter) {
							data[type].splice(0, 0, { isFilter: "isEmptyObjectFilter" });
						}

						for (let i = 0; i < data[type].length; i++) {
							let $storage = data[type][i].$storage || [];
							let $database = data[type][i].$database || [];
							let $array = data[type][i].$array || [];

							if (!Array.isArray($storage)) $storage = [data[type][i].$storage];
							if (!Array.isArray($database)) $database = [data[type][i].$database];
							if (!Array.isArray($array)) $array = [data[type][i].$array];

							if (!$storage.includes(storageName)) $storage.push(storageName);
							if (!$database.includes(database)) $database.push(database);
							if (!$array.includes(array)) $array.push(array);

							delete data[type][i].$storage;
							delete data[type][i].$database;
							delete data[type][i].$array;

							let queryFilter = isFilter ? data.$filter : (data[type][i].$filter || null);
							let { sqlWhere, params } = compileFilterToSQL(queryFilter, data.organization_id);

							if (method === "create") {
								data[type][i] = replaceArray(data[type][i]);
								data[type][i] = dotNotationToObject(data[type][i]);

								const id = data[type][i]._id || `row_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
								const orgId = data.organization_id;
								const created = {
									on: new Date(data.timeStamp),
									by: data.user_id || data.clientId
								};

								const recordPayload = { ...data[type][i] };
								delete recordPayload._id;
								delete recordPayload.organization_id;

								const sql = `
									INSERT INTO "${array}" (_id, organization_id, data, created, modified)
									VALUES (?, ?, ?, ?, NULL);
								`;
								const queryParams = [id, orgId, JSON.stringify(recordPayload), JSON.stringify(created)];

								process.emit("usage", {
									type: "egress",
									data: { sql, queryParams },
									organization_id: data.organization_id
								});

								await db.execute({ sql, args: queryParams });

								const createdDoc = await db.execute({
									sql: `SELECT * FROM "${array}" WHERE _id = ? AND organization_id = ?`,
									args: [id, orgId]
								});
								if (createdDoc.rows.length > 0) {
									const formattedDoc = parseSqlRow(createdDoc.rows[0]);
									data[type][i] = { ...formattedDoc, $storage, $database, $array };
								}
							}
							else if (method === "read") {
								let sql = `SELECT * FROM "${array}" ${sqlWhere}`;

								if (data.$filter && data.$filter.sort) {
									const sortClauses = data.$filter.sort.map(s => {
										let dir = s.direction === -1 || s.direction === "desc" ? "DESC" : "ASC";
										if (["_id", "organization_id"].includes(s.key)) {
											return `"${s.key}" ${dir}`;
										}
										return `json_extract(data, '$.${s.key}') ${dir}`;
									});
									sql += ` ORDER BY ${sortClauses.join(", ")}`;
								}
								if (data.$filter && typeof data.$filter.limit === "number") {
									sql += ` LIMIT ${data.$filter.limit}`;
								}
								if (data.$filter && typeof data.$filter.index === "number") {
									sql += ` OFFSET ${data.$filter.index}`;
								}

								if (data[type][i]._id && !isFilter) {
									sql = `SELECT * FROM "${array}" WHERE _id = ? AND organization_id = ?`;
									params = [data[type][i]._id, data.organization_id];
								}

								process.emit("usage", {
									type: "egress",
									data: { sql, params },
									organization_id: data.organization_id
								});

								const result = await db.execute({ sql, args: params });
								process.emit("usage", {
									type: "ingress",
									data: result.rows,
									organization_id: data.organization_id
								});

								if (isFilter) {
									for (let row of result.rows) {
										const parsedRow = parseSqlRow(row);
										if (data.$filter && data.$filter.search) {
											let isMatch = searchData(parsedRow, data.$filter.search);
											if (!isMatch) continue;
										}
										documents.push({
											...parsedRow,
											...reference
										});
									}
								} else if (result.rows.length > 0) {
									let dbRecord = parseSqlRow(result.rows[0]);

									if ($storage.length && data[type][i].modified && data[type][i].modified.on) {
										let clientTime = new Date(data[type][i].modified.on);
										let dbTime = new Date(dbRecord.modified ? dbRecord.modified.on : 0);

										if (clientTime > dbTime) {
											const updatedRecord = { ...dbRecord, ...data[type][i] };
											const savedRecord = await executeUpdateTransaction(db, array, data[type][i]._id, updatedRecord, data);
											data[type][i] = { ...savedRecord, $storage, $database, $array };
										} else {
											data[type][i] = { ...data[type][i], ...dbRecord, $storage, $database, $array };
										}
									} else {
										data[type][i] = {
											...data[type][i],
											...dbRecord,
											$storage, $database, $array
										};
									}
								} else {
									data[type].splice(i, 1);
									i--;
								}
							}
							else if (method === "update") {
								const updatePayload = { ...data[type][i] };
								delete updatePayload._id;
								delete updatePayload.$filter;

								if (data[type][i]._id) {
									const updatedDoc = await executeUpdateTransaction(db, array, data[type][i]._id, updatePayload, data);
									if (updatedDoc) {
										data[type][i] = { ...updatedDoc, $storage, $database, $array };
									}
								} else if (isFilter) {
									const selectSql = `SELECT _id FROM "${array}" ${sqlWhere}`;
									process.emit("usage", {
										type: "egress",
										data: { selectSql, params },
										organization_id: data.organization_id
									});
									const selectResult = await db.execute({ sql: selectSql, args: params });

									for (let row of selectResult.rows) {
										const updatedDoc = await executeUpdateTransaction(db, array, row._id, updatePayload, data);
										if (updatedDoc) {
											documents.push({ ...updatedDoc, ...reference });
										}
									}
								}
							}
							else if (method === "delete") {
								let sql, queryParams;
								if (data[type][i]._id) {
									sql = `DELETE FROM "${array}" WHERE _id = ? AND organization_id = ?;`;
									queryParams = [data[type][i]._id, data.organization_id];
								} else if (isFilter) {
									sql = `DELETE FROM "${array}" ${sqlWhere};`;
									queryParams = params;
								}

								if (sql) {
									process.emit("usage", {
										type: "egress",
										data: { sql, queryParams },
										organization_id: data.organization_id
									});
									const runResult = await db.execute({ sql, args: queryParams });
									process.emit("usage", {
										type: "ingress",
										data: runResult,
										organization_id: data.organization_id
									});

									if (isFilter) {
										documents.push({ _id: data[type][i]._id, ...reference });
									} else {
										data[type][i] = { _id: data[type][i]._id, $storage, $database, $array };
									}
								}
							}
						}
					}
				}

				data = createData(data, documents, type);
				resolve(data);
			} catch (error) {
				errorHandler(data, error);
				console.log(method, "error", error);
				resolve(data);
			}
		},
		(error) => {
			errorHandler(data, error);
		}
	);
}

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

export default { send };