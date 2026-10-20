import {
    ObjectId,
    dotNotationToObject,
    searchData,
    isValidDate
} from "@cocreate/utils";

import { 
    dbClient, 
    createData,
    errorHandler 
} from "./utilities.js";

/**
 * "database" operations mapped to SQLite Files (emulated catalogs).
 */
function database(method, data) {
    const storageName = data.storageConfig?.name;
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
						if (data.filter && data.filter.query) {
							let isFilter = queryData(dbObj, data.filter.query);
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

export default database