import {
    ObjectId,
    dotNotationToObject,
    searchData,
    isValidDate
} from "@cocreate/utils";

import { 
    dbClient, 
    createFilter, 
    createData,
    errorHandler 
} from "./utilities.js";

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

export default array