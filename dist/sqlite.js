import * as __WEBPACK_EXTERNAL_MODULE_http__ from "http";
import * as __WEBPACK_EXTERNAL_MODULE_https__ from "https";
import { createRequire as __WEBPACK_EXTERNAL_createRequire } from "node:module";
const __WEBPACK_EXTERNAL_createRequire_require = __WEBPACK_EXTERNAL_createRequire(import.meta.url);
/******/ var __webpack_modules__ = ({

/***/ 158
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   CI: () => (/* binding */ executeHranaBatch),
/* harmony export */   Er: () => (/* binding */ resultSetFromHrana),
/* harmony export */   LA: () => (/* binding */ mapHranaError),
/* harmony export */   WL: () => (/* binding */ stmtToHrana),
/* harmony export */   zB: () => (/* binding */ HranaTransaction)
/* harmony export */ });
/* harmony import */ var _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(5113);
/* harmony import */ var _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(6530);
/* harmony import */ var _libsql_core_util__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7372);



class HranaTransaction {
    #mode;
    #version;
    // Promise that is resolved when the BEGIN statement completes, or `undefined` if we haven't executed the
    // BEGIN statement yet.
    #started;
    /** @private */
    constructor(mode, version) {
        this.#mode = mode;
        this.#version = version;
        this.#started = undefined;
    }
    execute(stmt) {
        return this.batch([stmt]).then((results) => results[0]);
    }
    async batch(stmts) {
        const stream = this._getStream();
        if (stream.closed) {
            throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("Cannot execute statements because the transaction is closed", "TRANSACTION_CLOSED");
        }
        try {
            const hranaStmts = stmts.map(stmtToHrana);
            let rowsPromises;
            if (this.#started === undefined) {
                // The transaction hasn't started yet, so we need to send the BEGIN statement in a batch with
                // `hranaStmts`.
                this._getSqlCache().apply(hranaStmts);
                const batch = stream.batch(this.#version >= 3);
                const beginStep = batch.step();
                const beginPromise = beginStep.run((0,_libsql_core_util__WEBPACK_IMPORTED_MODULE_2__/* .transactionModeToBegin */ .UQ)(this.#mode));
                // Execute the `hranaStmts` only if the BEGIN succeeded, to make sure that we don't execute it
                // outside of a transaction.
                let lastStep = beginStep;
                rowsPromises = hranaStmts.map((hranaStmt) => {
                    const stmtStep = batch
                        .step()
                        .condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.ok(lastStep));
                    if (this.#version >= 3) {
                        // If the Hrana version supports it, make sure that we are still in a transaction
                        stmtStep.condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.not(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.isAutocommit(batch)));
                    }
                    const rowsPromise = stmtStep.query(hranaStmt);
                    rowsPromise.catch(() => undefined); // silence Node warning
                    lastStep = stmtStep;
                    return rowsPromise;
                });
                // `this.#started` is resolved successfully only if the batch and the BEGIN statement inside
                // of the batch are both successful.
                this.#started = batch
                    .execute()
                    .then(() => beginPromise)
                    .then(() => undefined);
                try {
                    await this.#started;
                }
                catch (e) {
                    // If the BEGIN failed, the transaction is unusable and we must close it. However, if the
                    // BEGIN suceeds and `hranaStmts` fail, the transaction is _not_ closed.
                    this.close();
                    throw e;
                }
            }
            else {
                if (this.#version < 3) {
                    // The transaction has started, so we must wait until the BEGIN statement completed to make
                    // sure that we don't execute `hranaStmts` outside of a transaction.
                    await this.#started;
                }
                else {
                    // The transaction has started, but we will use `hrana.BatchCond.isAutocommit()` to make
                    // sure that we don't execute `hranaStmts` outside of a transaction, so we don't have to
                    // wait for `this.#started`
                }
                this._getSqlCache().apply(hranaStmts);
                const batch = stream.batch(this.#version >= 3);
                let lastStep = undefined;
                rowsPromises = hranaStmts.map((hranaStmt) => {
                    const stmtStep = batch.step();
                    if (lastStep !== undefined) {
                        stmtStep.condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.ok(lastStep));
                    }
                    if (this.#version >= 3) {
                        stmtStep.condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.not(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.isAutocommit(batch)));
                    }
                    const rowsPromise = stmtStep.query(hranaStmt);
                    rowsPromise.catch(() => undefined); // silence Node warning
                    lastStep = stmtStep;
                    return rowsPromise;
                });
                await batch.execute();
            }
            const resultSets = [];
            for (const rowsPromise of rowsPromises) {
                const rows = await rowsPromise;
                if (rows === undefined) {
                    throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("Statement in a transaction was not executed, " +
                        "probably because the transaction has been rolled back", "TRANSACTION_CLOSED");
                }
                resultSets.push(resultSetFromHrana(rows));
            }
            return resultSets;
        }
        catch (e) {
            throw mapHranaError(e);
        }
    }
    async executeMultiple(sql) {
        const stream = this._getStream();
        if (stream.closed) {
            throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("Cannot execute statements because the transaction is closed", "TRANSACTION_CLOSED");
        }
        try {
            if (this.#started === undefined) {
                // If the transaction hasn't started yet, start it now
                this.#started = stream
                    .run((0,_libsql_core_util__WEBPACK_IMPORTED_MODULE_2__/* .transactionModeToBegin */ .UQ)(this.#mode))
                    .then(() => undefined);
                try {
                    await this.#started;
                }
                catch (e) {
                    this.close();
                    throw e;
                }
            }
            else {
                // Wait until the transaction has started
                await this.#started;
            }
            await stream.sequence(sql);
        }
        catch (e) {
            throw mapHranaError(e);
        }
    }
    async rollback() {
        try {
            const stream = this._getStream();
            if (stream.closed) {
                return;
            }
            if (this.#started !== undefined) {
                // We don't have to wait for the BEGIN statement to complete. If the BEGIN fails, we will
                // execute a ROLLBACK outside of an active transaction, which should be harmless.
            }
            else {
                // We did nothing in the transaction, so there is nothing to rollback.
                return;
            }
            // Pipeline the ROLLBACK statement and the stream close.
            const promise = stream.run("ROLLBACK").catch((e) => {
                throw mapHranaError(e);
            });
            stream.closeGracefully();
            await promise;
        }
        catch (e) {
            throw mapHranaError(e);
        }
        finally {
            // `this.close()` may close the `hrana.Client`, which aborts all pending stream requests, so we
            // must call it _after_ we receive the ROLLBACK response.
            // Also note that the current stream should already be closed, but we need to call `this.close()`
            // anyway, because it may need to do more cleanup.
            this.close();
        }
    }
    async commit() {
        // (this method is analogous to `rollback()`)
        try {
            const stream = this._getStream();
            if (stream.closed) {
                throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("Cannot commit the transaction because it is already closed", "TRANSACTION_CLOSED");
            }
            if (this.#started !== undefined) {
                // Make sure to execute the COMMIT only if the BEGIN was successful.
                await this.#started;
            }
            else {
                return;
            }
            const promise = stream.run("COMMIT").catch((e) => {
                throw mapHranaError(e);
            });
            stream.closeGracefully();
            await promise;
        }
        catch (e) {
            throw mapHranaError(e);
        }
        finally {
            this.close();
        }
    }
}
async function executeHranaBatch(mode, version, batch, hranaStmts) {
    const beginStep = batch.step();
    const beginPromise = beginStep.run((0,_libsql_core_util__WEBPACK_IMPORTED_MODULE_2__/* .transactionModeToBegin */ .UQ)(mode));
    let lastStep = beginStep;
    const stmtPromises = hranaStmts.map((hranaStmt) => {
        const stmtStep = batch.step().condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.ok(lastStep));
        if (version >= 3) {
            stmtStep.condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.not(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.isAutocommit(batch)));
        }
        const stmtPromise = stmtStep.query(hranaStmt);
        lastStep = stmtStep;
        return stmtPromise;
    });
    const commitStep = batch.step().condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.ok(lastStep));
    if (version >= 3) {
        commitStep.condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.not(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.isAutocommit(batch)));
    }
    const commitPromise = commitStep.run("COMMIT");
    const rollbackStep = batch
        .step()
        .condition(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.not(_libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .BatchCond */ .L8.ok(commitStep)));
    rollbackStep.run("ROLLBACK").catch((_) => undefined);
    await batch.execute();
    const resultSets = [];
    await beginPromise;
    for (const stmtPromise of stmtPromises) {
        const hranaRows = await stmtPromise;
        if (hranaRows === undefined) {
            throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("Statement in a batch was not executed, probably because the transaction has been rolled back", "TRANSACTION_CLOSED");
        }
        resultSets.push(resultSetFromHrana(hranaRows));
    }
    await commitPromise;
    return resultSets;
}
function stmtToHrana(stmt) {
    if (typeof stmt === "string") {
        return new _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .Stmt */ .XO(stmt);
    }
    const hranaStmt = new _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .Stmt */ .XO(stmt.sql);
    if (Array.isArray(stmt.args)) {
        hranaStmt.bindIndexes(stmt.args);
    }
    else {
        for (const [key, value] of Object.entries(stmt.args)) {
            hranaStmt.bindName(key, value);
        }
    }
    return hranaStmt;
}
function resultSetFromHrana(hranaRows) {
    const columns = hranaRows.columnNames.map((c) => c ?? "");
    const columnTypes = hranaRows.columnDecltypes.map((c) => c ?? "");
    const rows = hranaRows.rows;
    const rowsAffected = hranaRows.affectedRowCount;
    const lastInsertRowid = hranaRows.lastInsertRowid !== undefined
        ? hranaRows.lastInsertRowid
        : undefined;
    return new _libsql_core_util__WEBPACK_IMPORTED_MODULE_2__/* .ResultSetImpl */ .G9(columns, columnTypes, rows, rowsAffected, lastInsertRowid);
}
function mapHranaError(e) {
    if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .ClientError */ .MZ) {
        const code = mapHranaErrorCode(e);
        return new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m(e.message, code, undefined, e);
    }
    return e;
}
function mapHranaErrorCode(e) {
    if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .ResponseError */ .o3 && e.code !== undefined) {
        return e.code;
    }
    else if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO) {
        return "HRANA_PROTO_ERROR";
    }
    else if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .ClosedError */ .l8) {
        return e.cause instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .ClientError */ .MZ
            ? mapHranaErrorCode(e.cause)
            : "HRANA_CLOSED_ERROR";
    }
    else if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .WebSocketError */ .m5) {
        return "HRANA_WEBSOCKET_ERROR";
    }
    else if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .HttpServerError */ .Qt) {
        return "SERVER_ERROR";
    }
    else if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .ProtocolVersionError */ .XE) {
        return "PROTOCOL_VERSION_ERROR";
    }
    else if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .InternalError */ .Gd) {
        return "INTERNAL_ERROR";
    }
    else {
        return "UNKNOWN";
    }
}


/***/ },

/***/ 961
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   m: () => (/* binding */ waitForLastMigrationJobToFinish),
/* harmony export */   x: () => (/* binding */ getIsSchemaDatabase)
/* harmony export */ });
const SCHEMA_MIGRATION_SLEEP_TIME_IN_MS = 1000;
const SCHEMA_MIGRATION_MAX_RETRIES = 30;
async function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
async function isMigrationJobFinished({ authToken, baseUrl, jobId, }) {
    const url = normalizeURLScheme(baseUrl + `/v1/jobs/${jobId}`);
    const result = await fetch(url, {
        method: "GET",
        headers: {
            Authorization: `Bearer ${authToken}`,
        },
    });
    const json = (await result.json());
    const job = json;
    if (result.status !== 200) {
        throw new Error(`Unexpected status code while fetching job status for migration with id ${jobId}: ${result.status}`);
    }
    if (job.status == "RunFailure") {
        throw new Error("Migration job failed");
    }
    return job.status == "RunSuccess";
}
function normalizeURLScheme(url) {
    if (url.startsWith("ws://")) {
        return url.replace("ws://", "http://");
    }
    if (url.startsWith("wss://")) {
        return url.replace("wss://", "https://");
    }
    return url;
}
async function getIsSchemaDatabase({ authToken, baseUrl, }) {
    let responseStatusCode;
    try {
        if (baseUrl.startsWith("http://127.0.0.1")) {
            return false;
        }
        const url = normalizeURLScheme(baseUrl + "/v1/jobs");
        const result = await fetch(url, {
            method: "GET",
            headers: {
                Authorization: `Bearer ${authToken}`,
            },
        });
        if (result.status === 404 || result.status === 500) {
            return false;
        }
        const json = (await result.json());
        const isChildDatabase = result.status === 400 && json.error === "Invalid namespace";
        return !isChildDatabase;
    }
    catch (e) {
        console.error([
            `There has been an error while retrieving the database type.`,
            `Debug information:`,
            `- URL: ${baseUrl}`,
            `- Response Status Code: ${responseStatusCode ? responseStatusCode : "N/A"}`,
        ].join("\n"));
        throw e;
    }
}
async function getLastMigrationJob({ authToken, baseUrl, }) {
    const url = normalizeURLScheme(baseUrl + "/v1/jobs");
    const result = await fetch(url, {
        method: "GET",
        headers: {
            Authorization: `Bearer ${authToken}`,
        },
    });
    if (result.status !== 200) {
        throw new Error("Unexpected status code while fetching migration jobs: " +
            result.status);
    }
    const json = (await result.json());
    if (!json.migrations || json.migrations.length === 0) {
        throw new Error("No migrations found");
    }
    const migrations = json.migrations || [];
    let lastJob;
    for (const migration of migrations) {
        if (migration.job_id > (lastJob?.job_id || 0)) {
            lastJob = migration;
        }
    }
    if (!lastJob) {
        throw new Error("No migration job found");
    }
    if (lastJob?.status === "RunFailure") {
        throw new Error("Last migration job failed");
    }
    return lastJob;
}
async function waitForLastMigrationJobToFinish({ authToken, baseUrl, }) {
    const lastMigrationJob = await getLastMigrationJob({
        authToken: authToken,
        baseUrl,
    });
    if (lastMigrationJob.status !== "RunSuccess") {
        let i = 0;
        while (i < SCHEMA_MIGRATION_MAX_RETRIES) {
            i++;
            const isLastMigrationJobFinished = await isMigrationJobFinished({
                authToken: authToken,
                baseUrl,
                jobId: lastMigrationJob.job_id,
            });
            if (isLastMigrationJobFinished) {
                break;
            }
            await sleep(SCHEMA_MIGRATION_SLEEP_TIME_IN_MS);
        }
    }
}


/***/ },

/***/ 4391
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   L: () => (/* binding */ SqlCache)
/* harmony export */ });
class SqlCache {
    #owner;
    #sqls;
    capacity;
    constructor(owner, capacity) {
        this.#owner = owner;
        this.#sqls = new Lru();
        this.capacity = capacity;
    }
    // Replaces SQL strings with cached `hrana.Sql` objects in the statements in `hranaStmts`. After this
    // function returns, we guarantee that all `hranaStmts` refer to valid (not closed) `hrana.Sql` objects,
    // but _we may invalidate any other `hrana.Sql` objects_ (by closing them, thus removing them from the
    // server).
    //
    // In practice, this means that after calling this function, you can use the statements only up to the
    // first `await`, because concurrent code may also use the cache and invalidate those statements.
    apply(hranaStmts) {
        if (this.capacity <= 0) {
            return;
        }
        const usedSqlObjs = new Set();
        for (const hranaStmt of hranaStmts) {
            if (typeof hranaStmt.sql !== "string") {
                continue;
            }
            const sqlText = hranaStmt.sql;
            let sqlObj = this.#sqls.get(sqlText);
            if (sqlObj === undefined) {
                while (this.#sqls.size + 1 > this.capacity) {
                    const [evictSqlText, evictSqlObj] = this.#sqls.peekLru();
                    if (usedSqlObjs.has(evictSqlObj)) {
                        // The SQL object that we are trying to evict is already in use in this batch, so we
                        // must not evict and close it.
                        break;
                    }
                    evictSqlObj.close();
                    this.#sqls.delete(evictSqlText);
                }
                if (this.#sqls.size + 1 <= this.capacity) {
                    sqlObj = this.#owner.storeSql(sqlText);
                    this.#sqls.set(sqlText, sqlObj);
                }
            }
            if (sqlObj !== undefined) {
                hranaStmt.sql = sqlObj;
                usedSqlObjs.add(sqlObj);
            }
        }
    }
}
class Lru {
    // This maps keys to the cache values. The entries are ordered by their last use (entires that were used
    // most recently are at the end).
    #cache;
    constructor() {
        this.#cache = new Map();
    }
    get(key) {
        const value = this.#cache.get(key);
        if (value !== undefined) {
            // move the entry to the back of the Map
            this.#cache.delete(key);
            this.#cache.set(key, value);
        }
        return value;
    }
    set(key, value) {
        this.#cache.set(key, value);
    }
    peekLru() {
        for (const entry of this.#cache.entries()) {
            return entry;
        }
        return undefined;
    }
    delete(key) {
        this.#cache.delete(key);
    }
    get size() {
        return this.#cache.size;
    }
}


/***/ },

/***/ 6282
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   UU: () => (/* binding */ createClient)
/* harmony export */ });
/* unused harmony exports _createClient, WsClient, WsTransaction */
/* harmony import */ var _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(5113);
/* harmony import */ var _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(6530);
/* harmony import */ var _libsql_core_config__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(9082);
/* harmony import */ var _hrana_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(158);
/* harmony import */ var _sql_cache_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(4391);
/* harmony import */ var _libsql_core_uri__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(3920);
/* harmony import */ var _libsql_core_util__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(7372);
/* harmony import */ var _migrations_js__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(961);









function createClient(config) {
    return _createClient((0,_libsql_core_config__WEBPACK_IMPORTED_MODULE_2__/* .expandConfig */ .N)(config, false));
}
/** @private */
function _createClient(config) {
    if (config.scheme !== "wss" && config.scheme !== "ws") {
        throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m('The WebSocket client supports only "libsql:", "wss:" and "ws:" URLs, ' +
            `got ${JSON.stringify(config.scheme + ":")}. For more information, please read ${_libsql_core_util__WEBPACK_IMPORTED_MODULE_6__/* .supportedUrlLink */ .I2}`, "URL_SCHEME_NOT_SUPPORTED");
    }
    if (config.encryptionKey !== undefined) {
        throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("Encryption key is not supported by the remote client.", "ENCRYPTION_KEY_NOT_SUPPORTED");
    }
    if (config.scheme === "ws" && config.tls) {
        throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m(`A "ws:" URL cannot opt into TLS by using ?tls=1`, "URL_INVALID");
    }
    else if (config.scheme === "wss" && !config.tls) {
        throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m(`A "wss:" URL cannot opt out of TLS by using ?tls=0`, "URL_INVALID");
    }
    const url = (0,_libsql_core_uri__WEBPACK_IMPORTED_MODULE_5__/* .encodeBaseUrl */ .d)(config.scheme, config.authority, config.path);
    let client;
    try {
        client = _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .openWs */ .z8(url, config.authToken);
    }
    catch (e) {
        if (e instanceof _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .WebSocketUnsupportedError */ .Nb) {
            const suggestedScheme = config.scheme === "wss" ? "https" : "http";
            const suggestedUrl = (0,_libsql_core_uri__WEBPACK_IMPORTED_MODULE_5__/* .encodeBaseUrl */ .d)(suggestedScheme, config.authority, config.path);
            throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("This environment does not support WebSockets, please switch to the HTTP client by using " +
                `a "${suggestedScheme}:" URL (${JSON.stringify(suggestedUrl)}). ` +
                `For more information, please read ${_libsql_core_util__WEBPACK_IMPORTED_MODULE_6__/* .supportedUrlLink */ .I2}`, "WEBSOCKETS_NOT_SUPPORTED");
        }
        throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
    }
    return new WsClient(client, url, config.authToken, config.intMode);
}
const maxConnAgeMillis = 60 * 1000;
const sqlCacheCapacity = 100;
class WsClient {
    #url;
    #authToken;
    #intMode;
    // State of the current connection. The `hrana.WsClient` inside may be closed at any moment due to an
    // asynchronous error.
    #connState;
    // If defined, this is a connection that will be used in the future, once it is ready.
    #futureConnState;
    closed;
    protocol;
    #isSchemaDatabase;
    /** @private */
    constructor(client, url, authToken, intMode) {
        this.#url = url;
        this.#authToken = authToken;
        this.#intMode = intMode;
        this.#connState = this.#openConn(client);
        this.#futureConnState = undefined;
        this.closed = false;
        this.protocol = "ws";
    }
    async getIsSchemaDatabase() {
        if (this.#isSchemaDatabase === undefined) {
            this.#isSchemaDatabase = await (0,_migrations_js__WEBPACK_IMPORTED_MODULE_7__/* .getIsSchemaDatabase */ .x)({
                authToken: this.#authToken,
                baseUrl: this.#url.origin,
            });
        }
        return this.#isSchemaDatabase;
    }
    async execute(stmt) {
        const streamState = await this.#openStream();
        try {
            const isSchemaDatabasePromise = this.getIsSchemaDatabase();
            const hranaStmt = (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .stmtToHrana */ .WL)(stmt);
            // Schedule all operations synchronously, so they will be pipelined and executed in a single
            // network roundtrip.
            streamState.conn.sqlCache.apply([hranaStmt]);
            const hranaRowsPromise = streamState.stream.query(hranaStmt);
            streamState.stream.closeGracefully();
            const hranaRowsResult = await hranaRowsPromise;
            const isSchemaDatabase = await isSchemaDatabasePromise;
            if (isSchemaDatabase) {
                await (0,_migrations_js__WEBPACK_IMPORTED_MODULE_7__/* .waitForLastMigrationJobToFinish */ .m)({
                    authToken: this.#authToken,
                    baseUrl: this.#url.origin,
                });
            }
            return (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .resultSetFromHrana */ .Er)(hranaRowsResult);
        }
        catch (e) {
            throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
        }
        finally {
            this._closeStream(streamState);
        }
    }
    async batch(stmts, mode = "deferred") {
        const streamState = await this.#openStream();
        try {
            const isSchemaDatabasePromise = this.getIsSchemaDatabase();
            const hranaStmts = stmts.map(_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .stmtToHrana */ .WL);
            const version = await streamState.conn.client.getVersion();
            // Schedule all operations synchronously, so they will be pipelined and executed in a single
            // network roundtrip.
            streamState.conn.sqlCache.apply(hranaStmts);
            const batch = streamState.stream.batch(version >= 3);
            const resultsPromise = (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .executeHranaBatch */ .CI)(mode, version, batch, hranaStmts);
            const results = await resultsPromise;
            const isSchemaDatabase = await isSchemaDatabasePromise;
            if (isSchemaDatabase) {
                await (0,_migrations_js__WEBPACK_IMPORTED_MODULE_7__/* .waitForLastMigrationJobToFinish */ .m)({
                    authToken: this.#authToken,
                    baseUrl: this.#url.origin,
                });
            }
            return results;
        }
        catch (e) {
            throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
        }
        finally {
            this._closeStream(streamState);
        }
    }
    async transaction(mode = "write") {
        const streamState = await this.#openStream();
        try {
            const version = await streamState.conn.client.getVersion();
            // the BEGIN statement will be batched with the first statement on the transaction to save a
            // network roundtrip
            return new WsTransaction(this, streamState, mode, version);
        }
        catch (e) {
            this._closeStream(streamState);
            throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
        }
    }
    async executeMultiple(sql) {
        const streamState = await this.#openStream();
        try {
            // Schedule all operations synchronously, so they will be pipelined and executed in a single
            // network roundtrip.
            const promise = streamState.stream.sequence(sql);
            streamState.stream.closeGracefully();
            await promise;
        }
        catch (e) {
            throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
        }
        finally {
            this._closeStream(streamState);
        }
    }
    sync() {
        return Promise.resolve();
    }
    async #openStream() {
        if (this.closed) {
            throw new _libsql_core_api__WEBPACK_IMPORTED_MODULE_1__/* .LibsqlError */ .m("The client is closed", "CLIENT_CLOSED");
        }
        const now = new Date();
        const ageMillis = now.valueOf() - this.#connState.openTime.valueOf();
        if (ageMillis > maxConnAgeMillis &&
            this.#futureConnState === undefined) {
            // The existing connection is too old, let's open a new one.
            const futureConnState = this.#openConn();
            this.#futureConnState = futureConnState;
            // However, if we used `futureConnState` immediately, we would introduce additional latency,
            // because we would have to wait for the WebSocket handshake to complete, even though we may a
            // have perfectly good existing connection in `this.#connState`!
            //
            // So we wait until the `hrana.Client.getVersion()` operation completes (which happens when the
            // WebSocket hanshake completes), and only then we replace `this.#connState` with
            // `futureConnState`, which is stored in `this.#futureConnState` in the meantime.
            futureConnState.client.getVersion().then((_version) => {
                if (this.#connState !== futureConnState) {
                    // We need to close `this.#connState` before we replace it. However, it is possible
                    // that `this.#connState` has already been replaced: see the code below.
                    if (this.#connState.streamStates.size === 0) {
                        this.#connState.client.close();
                    }
                    else {
                        // If there are existing streams on the connection, we must not close it, because
                        // these streams would be broken. The last stream to be closed will also close the
                        // connection in `_closeStream()`.
                    }
                }
                this.#connState = futureConnState;
                this.#futureConnState = undefined;
            }, (_e) => {
                // If the new connection could not be established, let's just ignore the error and keep
                // using the existing connection.
                this.#futureConnState = undefined;
            });
        }
        if (this.#connState.client.closed) {
            // An error happened on this connection and it has been closed. Let's try to seamlessly reconnect.
            try {
                if (this.#futureConnState !== undefined) {
                    // We are already in the process of opening a new connection, so let's just use it
                    // immediately.
                    this.#connState = this.#futureConnState;
                }
                else {
                    this.#connState = this.#openConn();
                }
            }
            catch (e) {
                throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
            }
        }
        const connState = this.#connState;
        try {
            // Now we wait for the WebSocket handshake to complete (if it hasn't completed yet). Note that
            // this does not increase latency, because any messages that we would send on the WebSocket before
            // the handshake would be queued until the handshake is completed anyway.
            if (connState.useSqlCache === undefined) {
                connState.useSqlCache =
                    (await connState.client.getVersion()) >= 2;
                if (connState.useSqlCache) {
                    connState.sqlCache.capacity = sqlCacheCapacity;
                }
            }
            const stream = connState.client.openStream();
            stream.intMode = this.#intMode;
            const streamState = { conn: connState, stream };
            connState.streamStates.add(streamState);
            return streamState;
        }
        catch (e) {
            throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
        }
    }
    #openConn(client) {
        try {
            client ??= _libsql_hrana_client__WEBPACK_IMPORTED_MODULE_0__/* .openWs */ .z8(this.#url, this.#authToken);
            return {
                client,
                useSqlCache: undefined,
                sqlCache: new _sql_cache_js__WEBPACK_IMPORTED_MODULE_4__/* .SqlCache */ .L(client, 0),
                openTime: new Date(),
                streamStates: new Set(),
            };
        }
        catch (e) {
            throw (0,_hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .mapHranaError */ .LA)(e);
        }
    }
    _closeStream(streamState) {
        streamState.stream.close();
        const connState = streamState.conn;
        connState.streamStates.delete(streamState);
        if (connState.streamStates.size === 0 &&
            connState !== this.#connState) {
            // We are not using this connection anymore and this is the last stream that was using it, so we
            // must close it now.
            connState.client.close();
        }
    }
    close() {
        this.#connState.client.close();
        this.closed = true;
    }
}
class WsTransaction extends _hrana_js__WEBPACK_IMPORTED_MODULE_3__/* .HranaTransaction */ .zB {
    #client;
    #streamState;
    /** @private */
    constructor(client, state, mode, version) {
        super(mode, version);
        this.#client = client;
        this.#streamState = state;
    }
    /** @private */
    _getStream() {
        return this.#streamState.stream;
    }
    /** @private */
    _getSqlCache() {
        return this.#streamState.conn.sqlCache;
    }
    close() {
        this.#client._closeStream(this.#streamState);
    }
    get closed() {
        return this.#streamState.stream.closed;
    }
}


/***/ },

/***/ 6530
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   m: () => (/* binding */ LibsqlError)
/* harmony export */ });
/** Error thrown by the client. */
class LibsqlError extends Error {
    /** Machine-readable error code. */
    code;
    /** Raw numeric error code */
    rawCode;
    constructor(message, code, rawCode, cause) {
        if (code !== undefined) {
            message = `${code}: ${message}`;
        }
        super(message, { cause });
        this.code = code;
        this.rawCode = rawCode;
        this.name = "LibsqlError";
    }
}


/***/ },

/***/ 9082
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   N: () => (/* binding */ expandConfig)
/* harmony export */ });
/* harmony import */ var _api_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(6530);
/* harmony import */ var _uri_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3920);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7372);



function expandConfig(config, preferHttp) {
    if (typeof config !== "object") {
        // produce a reasonable error message in the common case where users type
        // `createClient("libsql://...")` instead of `createClient({url: "libsql://..."})`
        throw new TypeError(`Expected client configuration as object, got ${typeof config}`);
    }
    let tls = config.tls;
    let authToken = config.authToken;
    let encryptionKey = config.encryptionKey;
    let syncUrl = config.syncUrl;
    let syncInterval = config.syncInterval;
    const intMode = "" + (config.intMode ?? "number");
    if (intMode !== "number" && intMode !== "bigint" && intMode !== "string") {
        throw new TypeError(`Invalid value for intMode, expected "number", "bigint" or "string", \
            got ${JSON.stringify(intMode)}`);
    }
    if (config.url === ":memory:") {
        return {
            path: ":memory:",
            scheme: "file",
            syncUrl,
            syncInterval,
            intMode,
            fetch: config.fetch,
            tls: false,
            authToken: undefined,
            encryptionKey: undefined,
            authority: undefined,
        };
    }
    const uri = (0,_uri_js__WEBPACK_IMPORTED_MODULE_1__/* .parseUri */ .w)(config.url);
    for (const { key, value } of uri.query?.pairs ?? []) {
        if (key === "authToken") {
            authToken = value ? value : undefined;
        }
        else if (key === "tls") {
            if (value === "0") {
                tls = false;
            }
            else if (value === "1") {
                tls = true;
            }
            else {
                throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m(`Unknown value for the "tls" query argument: ${JSON.stringify(value)}. ` +
                    'Supported values are "0" and "1"', "URL_INVALID");
            }
        }
        else {
            throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m(`Unknown URL query parameter ${JSON.stringify(key)}`, "URL_PARAM_NOT_SUPPORTED");
        }
    }
    const uriScheme = uri.scheme.toLowerCase();
    let scheme;
    if (uriScheme === "libsql") {
        if (tls === false) {
            if (uri.authority?.port === undefined) {
                throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m('A "libsql:" URL with ?tls=0 must specify an explicit port', "URL_INVALID");
            }
            scheme = preferHttp ? "http" : "ws";
        }
        else {
            scheme = preferHttp ? "https" : "wss";
        }
    }
    else if (uriScheme === "http" || uriScheme === "ws") {
        scheme = uriScheme;
        tls ??= false;
    }
    else if (uriScheme === "https" ||
        uriScheme === "wss" ||
        uriScheme === "file") {
        scheme = uriScheme;
    }
    else {
        throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m('The client supports only "libsql:", "wss:", "ws:", "https:", "http:" and "file:" URLs, ' +
            `got ${JSON.stringify(uri.scheme + ":")}. ` +
            `For more information, please read ${_util_js__WEBPACK_IMPORTED_MODULE_2__/* .supportedUrlLink */ .I2}`, "URL_SCHEME_NOT_SUPPORTED");
    }
    if (uri.fragment !== undefined) {
        throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m(`URL fragments are not supported: ${JSON.stringify("#" + uri.fragment)}`, "URL_INVALID");
    }
    return {
        scheme,
        tls: tls ?? true,
        authority: uri.authority,
        path: uri.path,
        authToken,
        encryptionKey,
        syncUrl,
        syncInterval,
        intMode,
        fetch: config.fetch,
    };
}


/***/ },

/***/ 3920
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   d: () => (/* binding */ encodeBaseUrl),
/* harmony export */   w: () => (/* binding */ parseUri)
/* harmony export */ });
/* harmony import */ var _api_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(6530);
// URI parser based on RFC 3986
// We can't use the standard `URL` object, because we want to support relative `file:` URLs like
// `file:relative/path/database.db`, which are not correct according to RFC 8089, which standardizes the
// `file` scheme.

function parseUri(text) {
    const match = URI_RE.exec(text);
    if (match === null) {
        throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m("The URL is not in a valid format", "URL_INVALID");
    }
    const groups = match.groups;
    const scheme = groups["scheme"];
    const authority = groups["authority"] !== undefined
        ? parseAuthority(groups["authority"])
        : undefined;
    const path = percentDecode(groups["path"]);
    const query = groups["query"] !== undefined ? parseQuery(groups["query"]) : undefined;
    const fragment = groups["fragment"] !== undefined
        ? percentDecode(groups["fragment"])
        : undefined;
    return { scheme, authority, path, query, fragment };
}
const URI_RE = (() => {
    const SCHEME = "(?<scheme>[A-Za-z][A-Za-z.+-]*)";
    const AUTHORITY = "(?<authority>[^/?#]*)";
    const PATH = "(?<path>[^?#]*)";
    const QUERY = "(?<query>[^#]*)";
    const FRAGMENT = "(?<fragment>.*)";
    return new RegExp(`^${SCHEME}:(//${AUTHORITY})?${PATH}(\\?${QUERY})?(#${FRAGMENT})?$`, "su");
})();
function parseAuthority(text) {
    const match = AUTHORITY_RE.exec(text);
    if (match === null) {
        throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m("The authority part of the URL is not in a valid format", "URL_INVALID");
    }
    const groups = match.groups;
    const host = percentDecode(groups["host_br"] ?? groups["host"]);
    const port = groups["port"] ? parseInt(groups["port"], 10) : undefined;
    const userinfo = groups["username"] !== undefined
        ? {
            username: percentDecode(groups["username"]),
            password: groups["password"] !== undefined
                ? percentDecode(groups["password"])
                : undefined,
        }
        : undefined;
    return { host, port, userinfo };
}
const AUTHORITY_RE = (() => {
    return new RegExp(`^((?<username>[^:]*)(:(?<password>.*))?@)?((?<host>[^:\\[\\]]*)|(\\[(?<host_br>[^\\[\\]]*)\\]))(:(?<port>[0-9]*))?$`, "su");
})();
// Query string is parsed as application/x-www-form-urlencoded according to the Web URL standard:
// https://url.spec.whatwg.org/#urlencoded-parsing
function parseQuery(text) {
    const sequences = text.split("&");
    const pairs = [];
    for (const sequence of sequences) {
        if (sequence === "") {
            continue;
        }
        let key;
        let value;
        const splitIdx = sequence.indexOf("=");
        if (splitIdx < 0) {
            key = sequence;
            value = "";
        }
        else {
            key = sequence.substring(0, splitIdx);
            value = sequence.substring(splitIdx + 1);
        }
        pairs.push({
            key: percentDecode(key.replaceAll("+", " ")),
            value: percentDecode(value.replaceAll("+", " ")),
        });
    }
    return { pairs };
}
function percentDecode(text) {
    try {
        return decodeURIComponent(text);
    }
    catch (e) {
        if (e instanceof URIError) {
            throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m(`URL component has invalid percent encoding: ${e}`, "URL_INVALID", undefined, e);
        }
        throw e;
    }
}
function encodeBaseUrl(scheme, authority, path) {
    if (authority === undefined) {
        throw new _api_js__WEBPACK_IMPORTED_MODULE_0__/* .LibsqlError */ .m(`URL with scheme ${JSON.stringify(scheme + ":")} requires authority (the "//" part)`, "URL_INVALID");
    }
    const schemeText = `${scheme}:`;
    const hostText = encodeHost(authority.host);
    const portText = encodePort(authority.port);
    const userinfoText = encodeUserinfo(authority.userinfo);
    const authorityText = `//${userinfoText}${hostText}${portText}`;
    let pathText = path.split("/").map(encodeURIComponent).join("/");
    if (pathText !== "" && !pathText.startsWith("/")) {
        pathText = "/" + pathText;
    }
    return new URL(`${schemeText}${authorityText}${pathText}`);
}
function encodeHost(host) {
    return host.includes(":") ? `[${encodeURI(host)}]` : encodeURI(host);
}
function encodePort(port) {
    return port !== undefined ? `:${port}` : "";
}
function encodeUserinfo(userinfo) {
    if (userinfo === undefined) {
        return "";
    }
    const usernameText = encodeURIComponent(userinfo.username);
    const passwordText = userinfo.password !== undefined
        ? `:${encodeURIComponent(userinfo.password)}`
        : "";
    return `${usernameText}${passwordText}@`;
}


/***/ },

/***/ 7372
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   G9: () => (/* binding */ ResultSetImpl),
/* harmony export */   UQ: () => (/* binding */ transactionModeToBegin)
/* harmony export */ });
/* harmony import */ var js_base64__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(18);

const supportedUrlLink = "https://github.com/libsql/libsql-client-ts#supported-urls";
function transactionModeToBegin(mode) {
    if (mode === "write") {
        return "BEGIN IMMEDIATE";
    }
    else if (mode === "read") {
        return "BEGIN TRANSACTION READONLY";
    }
    else if (mode === "deferred") {
        return "BEGIN DEFERRED";
    }
    else {
        throw RangeError('Unknown transaction mode, supported values are "write", "read" and "deferred"');
    }
}
class ResultSetImpl {
    columns;
    columnTypes;
    rows;
    rowsAffected;
    lastInsertRowid;
    constructor(columns, columnTypes, rows, rowsAffected, lastInsertRowid) {
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.rows = rows;
        this.rowsAffected = rowsAffected;
        this.lastInsertRowid = lastInsertRowid;
    }
    toJSON() {
        return {
            columns: this.columns,
            columnTypes: this.columnTypes,
            rows: this.rows.map(rowToJson),
            rowsAffected: this.rowsAffected,
            lastInsertRowid: this.lastInsertRowid !== undefined
                ? "" + this.lastInsertRowid
                : null,
        };
    }
}
function rowToJson(row) {
    return Array.prototype.map.call(row, valueToJson);
}
function valueToJson(value) {
    if (typeof value === "bigint") {
        return "" + value;
    }
    else if (value instanceof ArrayBuffer) {
        return js_base64__WEBPACK_IMPORTED_MODULE_0__/* .Base64 */ .o4.fromUint8Array(new Uint8Array(value));
    }
    else {
        return value;
    }
}

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "I2", 0, /* binding */ supportedUrlLink
/* harmony export */ ]);


/***/ },

/***/ 2801
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   L8: () => (/* binding */ BatchCond),
/* harmony export */   lP: () => (/* binding */ Batch)
/* harmony export */ });
/* unused harmony export BatchStep */
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _result_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5464);
/* harmony import */ var _stmt_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2219);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5233);




/** A builder for creating a batch and executing it on the server. */
class Batch {
    /** @private */
    _stream;
    #useCursor;
    /** @private */
    _steps;
    #executed;
    /** @private */
    constructor(stream, useCursor) {
        this._stream = stream;
        this.#useCursor = useCursor;
        this._steps = [];
        this.#executed = false;
    }
    /** Return a builder for adding a step to the batch. */
    step() {
        return new BatchStep(this);
    }
    /** Execute the batch. */
    execute() {
        if (this.#executed) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .MisuseError */ .Xq("This batch has already been executed");
        }
        this.#executed = true;
        const batch = {
            steps: this._steps.map((step) => step.proto),
        };
        if (this.#useCursor) {
            return executeCursor(this._stream, this._steps, batch);
        }
        else {
            return executeRegular(this._stream, this._steps, batch);
        }
    }
}
function executeRegular(stream, steps, batch) {
    return stream._batch(batch).then((result) => {
        for (let step = 0; step < steps.length; ++step) {
            const stepResult = result.stepResults.get(step);
            const stepError = result.stepErrors.get(step);
            steps[step].callback(stepResult, stepError);
        }
    });
}
async function executeCursor(stream, steps, batch) {
    const cursor = await stream._openCursor(batch);
    try {
        let nextStep = 0;
        let beginEntry = undefined;
        let rows = [];
        for (;;) {
            const entry = await cursor.next();
            if (entry === undefined) {
                break;
            }
            if (entry.type === "step_begin") {
                if (entry.step < nextStep || entry.step >= steps.length) {
                    throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced StepBeginEntry for unexpected step");
                }
                else if (beginEntry !== undefined) {
                    throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced StepBeginEntry before terminating previous step");
                }
                for (let step = nextStep; step < entry.step; ++step) {
                    steps[step].callback(undefined, undefined);
                }
                nextStep = entry.step + 1;
                beginEntry = entry;
                rows = [];
            }
            else if (entry.type === "step_end") {
                if (beginEntry === undefined) {
                    throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced StepEndEntry but no step is active");
                }
                const stmtResult = {
                    cols: beginEntry.cols,
                    rows,
                    affectedRowCount: entry.affectedRowCount,
                    lastInsertRowid: entry.lastInsertRowid,
                };
                steps[beginEntry.step].callback(stmtResult, undefined);
                beginEntry = undefined;
                rows = [];
            }
            else if (entry.type === "step_error") {
                if (beginEntry === undefined) {
                    if (entry.step >= steps.length) {
                        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced StepErrorEntry for unexpected step");
                    }
                    for (let step = nextStep; step < entry.step; ++step) {
                        steps[step].callback(undefined, undefined);
                    }
                }
                else {
                    if (entry.step !== beginEntry.step) {
                        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced StepErrorEntry for unexpected step");
                    }
                    beginEntry = undefined;
                    rows = [];
                }
                steps[entry.step].callback(undefined, entry.error);
                nextStep = entry.step + 1;
            }
            else if (entry.type === "row") {
                if (beginEntry === undefined) {
                    throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced RowEntry but no step is active");
                }
                rows.push(entry.row);
            }
            else if (entry.type === "error") {
                throw (0,_result_js__WEBPACK_IMPORTED_MODULE_1__/* .errorFromProto */ .NC)(entry.error);
            }
            else if (entry.type === "none") {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server produced unrecognized CursorEntry");
            }
            else {
                throw (0,_util_js__WEBPACK_IMPORTED_MODULE_3__/* .impossible */ .y)(entry, "Impossible CursorEntry");
            }
        }
        if (beginEntry !== undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server closed Cursor before terminating active step");
        }
        for (let step = nextStep; step < steps.length; ++step) {
            steps[step].callback(undefined, undefined);
        }
    }
    finally {
        cursor.close();
    }
}
/** A builder for adding a step to the batch. */
class BatchStep {
    /** @private */
    _batch;
    #conds;
    /** @private */
    _index;
    /** @private */
    constructor(batch) {
        this._batch = batch;
        this.#conds = [];
        this._index = undefined;
    }
    /** Add the condition that needs to be satisfied to execute the statement. If you use this method multiple
     * times, we join the conditions with a logical AND. */
    condition(cond) {
        this.#conds.push(cond._proto);
        return this;
    }
    /** Add a statement that returns rows. */
    query(stmt) {
        return this.#add(stmt, true, _result_js__WEBPACK_IMPORTED_MODULE_1__/* .rowsResultFromProto */ .tc);
    }
    /** Add a statement that returns at most a single row. */
    queryRow(stmt) {
        return this.#add(stmt, true, _result_js__WEBPACK_IMPORTED_MODULE_1__/* .rowResultFromProto */ .SC);
    }
    /** Add a statement that returns at most a single value. */
    queryValue(stmt) {
        return this.#add(stmt, true, _result_js__WEBPACK_IMPORTED_MODULE_1__/* .valueResultFromProto */ .j8);
    }
    /** Add a statement without returning rows. */
    run(stmt) {
        return this.#add(stmt, false, _result_js__WEBPACK_IMPORTED_MODULE_1__/* .stmtResultFromProto */ .sR);
    }
    #add(inStmt, wantRows, fromProto) {
        if (this._index !== undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .MisuseError */ .Xq("This BatchStep has already been added to the batch");
        }
        const stmt = (0,_stmt_js__WEBPACK_IMPORTED_MODULE_2__/* .stmtToProto */ .O)(this._batch._stream._sqlOwner(), inStmt, wantRows);
        let condition;
        if (this.#conds.length === 0) {
            condition = undefined;
        }
        else if (this.#conds.length === 1) {
            condition = this.#conds[0];
        }
        else {
            condition = { type: "and", conds: this.#conds.slice() };
        }
        const proto = { stmt, condition };
        return new Promise((outputCallback, errorCallback) => {
            const callback = (stepResult, stepError) => {
                if (stepResult !== undefined && stepError !== undefined) {
                    errorCallback(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Server returned both result and error"));
                }
                else if (stepError !== undefined) {
                    errorCallback((0,_result_js__WEBPACK_IMPORTED_MODULE_1__/* .errorFromProto */ .NC)(stepError));
                }
                else if (stepResult !== undefined) {
                    outputCallback(fromProto(stepResult, this._batch._stream.intMode));
                }
                else {
                    outputCallback(undefined);
                }
            };
            this._index = this._batch._steps.length;
            this._batch._steps.push({ proto, callback });
        });
    }
}
class BatchCond {
    /** @private */
    _batch;
    /** @private */
    _proto;
    /** @private */
    constructor(batch, proto) {
        this._batch = batch;
        this._proto = proto;
    }
    /** Create a condition that evaluates to true when the given step executes successfully.
     *
     * If the given step fails error or is skipped because its condition evaluated to false, this
     * condition evaluates to false.
     */
    static ok(step) {
        return new BatchCond(step._batch, { type: "ok", step: stepIndex(step) });
    }
    /** Create a condition that evaluates to true when the given step fails.
     *
     * If the given step succeeds or is skipped because its condition evaluated to false, this condition
     * evaluates to false.
     */
    static error(step) {
        return new BatchCond(step._batch, { type: "error", step: stepIndex(step) });
    }
    /** Create a condition that is a logical negation of another condition.
     */
    static not(cond) {
        return new BatchCond(cond._batch, { type: "not", cond: cond._proto });
    }
    /** Create a condition that is a logical AND of other conditions.
     */
    static and(batch, conds) {
        for (const cond of conds) {
            checkCondBatch(batch, cond);
        }
        return new BatchCond(batch, { type: "and", conds: conds.map(e => e._proto) });
    }
    /** Create a condition that is a logical OR of other conditions.
     */
    static or(batch, conds) {
        for (const cond of conds) {
            checkCondBatch(batch, cond);
        }
        return new BatchCond(batch, { type: "or", conds: conds.map(e => e._proto) });
    }
    /** Create a condition that evaluates to true when the SQL connection is in autocommit mode (not inside an
     * explicit transaction). This requires protocol version 3 or higher.
     */
    static isAutocommit(batch) {
        batch._stream.client()._ensureVersion(3, "BatchCond.isAutocommit()");
        return new BatchCond(batch, { type: "is_autocommit" });
    }
}
function stepIndex(step) {
    if (step._index === undefined) {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .MisuseError */ .Xq("Cannot add a condition referencing a step that has not been added to the batch");
    }
    return step._index;
}
function checkCondBatch(expectedBatch, cond) {
    if (cond._batch !== expectedBatch) {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .MisuseError */ .Xq("Cannot mix BatchCond objects for different Batch objects");
    }
}


/***/ },

/***/ 4375
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   y: () => (/* binding */ ByteQueue)
/* harmony export */ });
class ByteQueue {
    #array;
    #shiftPos;
    #pushPos;
    constructor(initialCap) {
        this.#array = new Uint8Array(new ArrayBuffer(initialCap));
        this.#shiftPos = 0;
        this.#pushPos = 0;
    }
    get length() {
        return this.#pushPos - this.#shiftPos;
    }
    data() {
        return this.#array.slice(this.#shiftPos, this.#pushPos);
    }
    push(chunk) {
        this.#ensurePush(chunk.byteLength);
        this.#array.set(chunk, this.#pushPos);
        this.#pushPos += chunk.byteLength;
    }
    #ensurePush(pushLength) {
        if (this.#pushPos + pushLength <= this.#array.byteLength) {
            return;
        }
        const filledLength = this.#pushPos - this.#shiftPos;
        if (filledLength + pushLength <= this.#array.byteLength &&
            2 * this.#pushPos >= this.#array.byteLength) {
            this.#array.copyWithin(0, this.#shiftPos, this.#pushPos);
        }
        else {
            let newCap = this.#array.byteLength;
            do {
                newCap *= 2;
            } while (filledLength + pushLength > newCap);
            const newArray = new Uint8Array(new ArrayBuffer(newCap));
            newArray.set(this.#array.slice(this.#shiftPos, this.#pushPos), 0);
            this.#array = newArray;
        }
        this.#pushPos = filledLength;
        this.#shiftPos = 0;
    }
    shift(length) {
        this.#shiftPos += length;
    }
}


/***/ },

/***/ 4930
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   K: () => (/* binding */ Client)
/* harmony export */ });
/** A client for the Hrana protocol (a "database connection pool"). */
class Client {
    /** @private */
    constructor() {
        this.intMode = "number";
    }
    /** Representation of integers returned from the database. See {@link IntMode}.
     *
     * This value is inherited by {@link Stream} objects created with {@link openStream}, but you can
     * override the integer mode for every stream by setting {@link Stream.intMode} on the stream.
     */
    intMode;
}


/***/ },

/***/ 1815
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   b: () => (/* binding */ Cursor)
/* harmony export */ });
class Cursor {
}


/***/ },

/***/ 6264
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   b: () => (/* binding */ describeResultFromProto)
/* harmony export */ });
function describeResultFromProto(result) {
    return {
        paramNames: result.params.map((p) => p.name),
        columns: result.cols,
        isExplain: result.isExplain,
        isReadonly: result.isReadonly,
    };
}


/***/ },

/***/ 7511
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   BM: () => (/* reexport safe */ _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_2__.B),
/* harmony export */   jL: () => (/* reexport safe */ _json_encode_js__WEBPACK_IMPORTED_MODULE_1__.j),
/* harmony export */   op: () => (/* reexport safe */ _json_decode_js__WEBPACK_IMPORTED_MODULE_0__.op),
/* harmony export */   s_: () => (/* reexport safe */ _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_3__.s)
/* harmony export */ });
/* harmony import */ var _json_decode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(3806);
/* harmony import */ var _json_encode_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(1282);
/* harmony import */ var _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(5921);
/* harmony import */ var _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(7929);






/***/ },

/***/ 3806
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Ik: () => (/* binding */ object),
/* harmony export */   Ki: () => (/* binding */ arrayObjectsMap),
/* harmony export */   YO: () => (/* binding */ array),
/* harmony export */   Yj: () => (/* binding */ string),
/* harmony export */   ai: () => (/* binding */ number),
/* harmony export */   op: () => (/* binding */ readJsonObject),
/* harmony export */   rJ: () => (/* binding */ stringOpt),
/* harmony export */   zM: () => (/* binding */ boolean)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);

function string(value) {
    if (typeof value === "string") {
        return value;
    }
    throw typeError(value, "string");
}
function stringOpt(value) {
    if (value === null || value === undefined) {
        return undefined;
    }
    else if (typeof value === "string") {
        return value;
    }
    throw typeError(value, "string or null");
}
function number(value) {
    if (typeof value === "number") {
        return value;
    }
    throw typeError(value, "number");
}
function boolean(value) {
    if (typeof value === "boolean") {
        return value;
    }
    throw typeError(value, "boolean");
}
function array(value) {
    if (Array.isArray(value)) {
        return value;
    }
    throw typeError(value, "array");
}
function object(value) {
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
        return value;
    }
    throw typeError(value, "object");
}
function arrayObjectsMap(value, fun) {
    return array(value).map((elemValue) => fun(object(elemValue)));
}
function typeError(value, expected) {
    if (value === undefined) {
        return new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO(`Expected ${expected}, but the property was missing`);
    }
    let received = typeof value;
    if (value === null) {
        received = "null";
    }
    else if (Array.isArray(value)) {
        received = "array";
    }
    return new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO(`Expected ${expected}, received ${received}`);
}
function readJsonObject(value, fun) {
    return fun(object(value));
}


/***/ },

/***/ 1282
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   j: () => (/* binding */ writeJsonObject)
/* harmony export */ });
/* unused harmony export ObjectWriter */
class ObjectWriter {
    #output;
    #isFirst;
    constructor(output) {
        this.#output = output;
        this.#isFirst = false;
    }
    begin() {
        this.#output.push('{');
        this.#isFirst = true;
    }
    end() {
        this.#output.push('}');
        this.#isFirst = false;
    }
    #key(name) {
        if (this.#isFirst) {
            this.#output.push('"');
            this.#isFirst = false;
        }
        else {
            this.#output.push(',"');
        }
        this.#output.push(name);
        this.#output.push('":');
    }
    string(name, value) {
        this.#key(name);
        this.#output.push(JSON.stringify(value));
    }
    stringRaw(name, value) {
        this.#key(name);
        this.#output.push('"');
        this.#output.push(value);
        this.#output.push('"');
    }
    number(name, value) {
        this.#key(name);
        this.#output.push("" + value);
    }
    boolean(name, value) {
        this.#key(name);
        this.#output.push(value ? "true" : "false");
    }
    object(name, value, valueFun) {
        this.#key(name);
        this.begin();
        valueFun(this, value);
        this.end();
    }
    arrayObjects(name, values, valueFun) {
        this.#key(name);
        this.#output.push('[');
        for (let i = 0; i < values.length; ++i) {
            if (i !== 0) {
                this.#output.push(',');
            }
            this.begin();
            valueFun(this, values[i]);
            this.end();
        }
        this.#output.push(']');
    }
}
function writeJsonObject(value, fun) {
    const output = [];
    const writer = new ObjectWriter(output);
    writer.begin();
    fun(writer, value);
    writer.end();
    return output.join("");
}


/***/ },

/***/ 5921
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   B: () => (/* binding */ readProtobufMessage)
/* harmony export */ });
/* unused harmony export FieldReader */
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);


class MessageReader {
    #array;
    #view;
    #pos;
    constructor(array) {
        this.#array = array;
        this.#view = new DataView(array.buffer, array.byteOffset, array.byteLength);
        this.#pos = 0;
    }
    varint() {
        let value = 0;
        for (let shift = 0;; shift += 7) {
            const byte = this.#array[this.#pos++];
            value |= (byte & 0x7f) << shift;
            if (!(byte & 0x80)) {
                break;
            }
        }
        return value;
    }
    varintBig() {
        let value = 0n;
        for (let shift = 0n;; shift += 7n) {
            const byte = this.#array[this.#pos++];
            value |= BigInt(byte & 0x7f) << shift;
            if (!(byte & 0x80)) {
                break;
            }
        }
        return value;
    }
    bytes(length) {
        const array = new Uint8Array(this.#array.buffer, this.#array.byteOffset + this.#pos, length);
        this.#pos += length;
        return array;
    }
    double() {
        const value = this.#view.getFloat64(this.#pos, true);
        this.#pos += 8;
        return value;
    }
    skipVarint() {
        for (;;) {
            const byte = this.#array[this.#pos++];
            if (!(byte & 0x80)) {
                break;
            }
        }
    }
    skip(count) {
        this.#pos += count;
    }
    eof() {
        return this.#pos >= this.#array.byteLength;
    }
}
class FieldReader {
    #reader;
    #wireType;
    constructor(reader) {
        this.#reader = reader;
        this.#wireType = -1;
    }
    setup(wireType) {
        this.#wireType = wireType;
    }
    #expect(expectedWireType) {
        if (this.#wireType !== expectedWireType) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO(`Expected wire type ${expectedWireType}, got ${this.#wireType}`);
        }
        this.#wireType = -1;
    }
    bytes() {
        this.#expect((/* inlined export .LENGTH_DELIMITED */2));
        const length = this.#reader.varint();
        return this.#reader.bytes(length);
    }
    string() {
        return new TextDecoder().decode(this.bytes());
    }
    message(def) {
        return readProtobufMessage(this.bytes(), def);
    }
    int32() {
        this.#expect((/* inlined export .VARINT */0));
        return this.#reader.varint();
    }
    uint32() {
        return this.int32();
    }
    bool() {
        return this.int32() !== 0;
    }
    uint64() {
        this.#expect((/* inlined export .VARINT */0));
        return this.#reader.varintBig();
    }
    sint64() {
        const value = this.uint64();
        return (value >> 1n) ^ (-(value & 1n));
    }
    double() {
        this.#expect((/* inlined export .FIXED_64 */1));
        return this.#reader.double();
    }
    maybeSkip() {
        if (this.#wireType < 0) {
            return;
        }
        else if (this.#wireType === (/* inlined export .VARINT */0)) {
            this.#reader.skipVarint();
        }
        else if (this.#wireType === (/* inlined export .FIXED_64 */1)) {
            this.#reader.skip(8);
        }
        else if (this.#wireType === (/* inlined export .LENGTH_DELIMITED */2)) {
            const length = this.#reader.varint();
            this.#reader.skip(length);
        }
        else if (this.#wireType === (/* inlined export .FIXED_32 */5)) {
            this.#reader.skip(4);
        }
        else {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO(`Unexpected wire type ${this.#wireType}`);
        }
        this.#wireType = -1;
    }
}
function readProtobufMessage(data, def) {
    const msgReader = new MessageReader(data);
    const fieldReader = new FieldReader(msgReader);
    let value = def.default();
    while (!msgReader.eof()) {
        const key = msgReader.varint();
        const tag = key >> 3;
        const wireType = key & 0x7;
        fieldReader.setup(wireType);
        const tagFun = def[tag];
        if (tagFun !== undefined) {
            const returnedValue = tagFun(fieldReader, value);
            if (returnedValue !== undefined) {
                value = returnedValue;
            }
        }
        fieldReader.maybeSkip();
    }
    return value;
}


/***/ },

/***/ 7929
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   s: () => (/* binding */ writeProtobufMessage)
/* harmony export */ });
/* unused harmony export MessageWriter */

class MessageWriter {
    #buf;
    #array;
    #view;
    #pos;
    constructor() {
        this.#buf = new ArrayBuffer(256);
        this.#array = new Uint8Array(this.#buf);
        this.#view = new DataView(this.#buf);
        this.#pos = 0;
    }
    #ensure(extra) {
        if (this.#pos + extra <= this.#buf.byteLength) {
            return;
        }
        let newCap = this.#buf.byteLength;
        while (newCap < this.#pos + extra) {
            newCap *= 2;
        }
        const newBuf = new ArrayBuffer(newCap);
        const newArray = new Uint8Array(newBuf);
        const newView = new DataView(newBuf);
        newArray.set(new Uint8Array(this.#buf, 0, this.#pos));
        this.#buf = newBuf;
        this.#array = newArray;
        this.#view = newView;
    }
    #varint(value) {
        this.#ensure(5);
        value = 0 | value;
        do {
            let byte = value & 0x7f;
            value >>>= 7;
            byte |= (value ? 0x80 : 0);
            this.#array[this.#pos++] = byte;
        } while (value);
    }
    #varintBig(value) {
        this.#ensure(10);
        value = value & 0xffffffffffffffffn;
        do {
            let byte = Number(value & 0x7fn);
            value >>= 7n;
            byte |= (value ? 0x80 : 0);
            this.#array[this.#pos++] = byte;
        } while (value);
    }
    #tag(tag, wireType) {
        this.#varint((tag << 3) | wireType);
    }
    bytes(tag, value) {
        this.#tag(tag, (/* inlined export .LENGTH_DELIMITED */2));
        this.#varint(value.byteLength);
        this.#ensure(value.byteLength);
        this.#array.set(value, this.#pos);
        this.#pos += value.byteLength;
    }
    string(tag, value) {
        this.bytes(tag, new TextEncoder().encode(value));
    }
    message(tag, value, fun) {
        const writer = new MessageWriter();
        fun(writer, value);
        this.bytes(tag, writer.data());
    }
    int32(tag, value) {
        this.#tag(tag, (/* inlined export .VARINT */0));
        this.#varint(value);
    }
    uint32(tag, value) {
        this.int32(tag, value);
    }
    bool(tag, value) {
        this.int32(tag, value ? 1 : 0);
    }
    sint64(tag, value) {
        this.#tag(tag, (/* inlined export .VARINT */0));
        this.#varintBig((value << 1n) ^ (value >> 63n));
    }
    double(tag, value) {
        this.#tag(tag, (/* inlined export .FIXED_64 */1));
        this.#ensure(8);
        this.#view.setFloat64(this.#pos, value, true);
        this.#pos += 8;
    }
    data() {
        return new Uint8Array(this.#buf, 0, this.#pos);
    }
}
function writeProtobufMessage(value, fun) {
    const w = new MessageWriter();
    fun(w, value);
    return w.data();
}


/***/ },

/***/ 8910
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Gd: () => (/* binding */ InternalError),
/* harmony export */   MZ: () => (/* binding */ ClientError),
/* harmony export */   Nb: () => (/* binding */ WebSocketUnsupportedError),
/* harmony export */   Qt: () => (/* binding */ HttpServerError),
/* harmony export */   XE: () => (/* binding */ ProtocolVersionError),
/* harmony export */   Xq: () => (/* binding */ MisuseError),
/* harmony export */   l8: () => (/* binding */ ClosedError),
/* harmony export */   m5: () => (/* binding */ WebSocketError),
/* harmony export */   o3: () => (/* binding */ ResponseError),
/* harmony export */   pO: () => (/* binding */ ProtoError)
/* harmony export */ });
/* unused harmony export LibsqlUrlParseError */
/** Generic error produced by the Hrana client. */
class ClientError extends Error {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "ClientError";
    }
}
/** Error thrown when the server violates the protocol. */
class ProtoError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "ProtoError";
    }
}
/** Error thrown when the server returns an error response. */
class ResponseError extends ClientError {
    code;
    /** @internal */
    proto;
    /** @private */
    constructor(message, protoError) {
        super(message);
        this.name = "ResponseError";
        this.code = protoError.code;
        this.proto = protoError;
        this.stack = undefined;
    }
}
/** Error thrown when the client or stream is closed. */
class ClosedError extends ClientError {
    /** @private */
    constructor(message, cause) {
        if (cause !== undefined) {
            super(`${message}: ${cause}`);
            this.cause = cause;
        }
        else {
            super(message);
        }
        this.name = "ClosedError";
    }
}
/** Error thrown when the environment does not seem to support WebSockets. */
class WebSocketUnsupportedError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "WebSocketUnsupportedError";
    }
}
/** Error thrown when we encounter a WebSocket error. */
class WebSocketError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "WebSocketError";
    }
}
/** Error thrown when the HTTP server returns an error response. */
class HttpServerError extends ClientError {
    status;
    /** @private */
    constructor(message, status) {
        super(message);
        this.status = status;
        this.name = "HttpServerError";
    }
}
/** Error thrown when a libsql URL is not valid. */
class LibsqlUrlParseError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "LibsqlUrlParseError";
    }
}
/** Error thrown when the protocol version is too low to support a feature. */
class ProtocolVersionError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "ProtocolVersionError";
    }
}
/** Error thrown when an internal client error happens. */
class InternalError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "InternalError";
    }
}
/** Error thrown when the API is misused. */
class MisuseError extends ClientError {
    /** @private */
    constructor(message) {
        super(message);
        this.name = "MisuseError";
    }
}


/***/ },

/***/ 1911
(__unused_webpack_module, __unused_webpack___webpack_exports__, __webpack_require__) {

/* unused harmony exports checkEndpoints, HttpClient */
/* harmony import */ var _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(451);
/* harmony import */ var _client_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(4930);
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(8910);
/* harmony import */ var _stream_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(8348);




const checkEndpoints = [
    {
        versionPath: "v3-protobuf",
        pipelinePath: "v3-protobuf/pipeline",
        cursorPath: "v3-protobuf/cursor",
        version: 3,
        encoding: "protobuf",
    },
    /*
    {
        versionPath: "v3",
        pipelinePath: "v3/pipeline",
        cursorPath: "v3/cursor",
        version: 3,
        encoding: "json",
    },
    */
];
const fallbackEndpoint = {
    versionPath: "v2",
    pipelinePath: "v2/pipeline",
    cursorPath: undefined,
    version: 2,
    encoding: "json",
};
/** A client for the Hrana protocol over HTTP. */
class HttpClient extends _client_js__WEBPACK_IMPORTED_MODULE_1__/* .Client */ .K {
    #url;
    #jwt;
    #fetch;
    #closed;
    #streams;
    /** @private */
    _endpointPromise;
    /** @private */
    _endpoint;
    /** @private */
    constructor(url, jwt, customFetch, protocolVersion = 2) {
        super();
        this.#url = url;
        this.#jwt = jwt;
        this.#fetch = customFetch ?? _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_0__/* .fetch */ .hd;
        this.#closed = undefined;
        this.#streams = new Set();
        if (protocolVersion == 3) {
            this._endpointPromise = findEndpoint(this.#fetch, this.#url);
            this._endpointPromise.then((endpoint) => this._endpoint = endpoint, (error) => this.#setClosed(error));
        }
        else {
            this._endpointPromise = Promise.resolve(fallbackEndpoint);
            this._endpointPromise.then((endpoint) => this._endpoint = endpoint, (error) => this.#setClosed(error));
        }
    }
    /** Get the protocol version supported by the server. */
    async getVersion() {
        if (this._endpoint !== undefined) {
            return this._endpoint.version;
        }
        return (await this._endpointPromise).version;
    }
    // Make sure that the negotiated version is at least `minVersion`.
    /** @private */
    _ensureVersion(minVersion, feature) {
        if (minVersion <= fallbackEndpoint.version) {
            return;
        }
        else if (this._endpoint === undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtocolVersionError */ .XE(`${feature} is supported only on protocol version ${minVersion} and higher, ` +
                "but the version supported by the HTTP server is not yet known. " +
                "Use Client.getVersion() to wait until the version is available.");
        }
        else if (this._endpoint.version < minVersion) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtocolVersionError */ .XE(`${feature} is supported only on protocol version ${minVersion} and higher, ` +
                `but the HTTP server only supports version ${this._endpoint.version}.`);
        }
    }
    /** Open a {@link HttpStream}, a stream for executing SQL statements. */
    openStream() {
        if (this.#closed !== undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ClosedError */ .l8("Client is closed", this.#closed);
        }
        const stream = new _stream_js__WEBPACK_IMPORTED_MODULE_3__/* .HttpStream */ .P(this, this.#url, this.#jwt, this.#fetch);
        this.#streams.add(stream);
        return stream;
    }
    /** @private */
    _streamClosed(stream) {
        this.#streams.delete(stream);
    }
    /** Close the client and all its streams. */
    close() {
        this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ClientError */ .MZ("Client was manually closed"));
    }
    /** True if the client is closed. */
    get closed() {
        return this.#closed !== undefined;
    }
    #setClosed(error) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        for (const stream of Array.from(this.#streams)) {
            stream._setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ClosedError */ .l8("Client was closed", error));
        }
    }
}
async function findEndpoint(customFetch, clientUrl) {
    const fetch = customFetch;
    for (const endpoint of checkEndpoints) {
        const url = new URL(endpoint.versionPath, clientUrl);
        const request = new _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_0__/* .Request */ .Kd(url.toString(), { method: "GET" });
        const response = await fetch(request);
        await response.arrayBuffer();
        if (response.ok) {
            return endpoint;
        }
    }
    return fallbackEndpoint;
}


/***/ },

/***/ 8342
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   h: () => (/* binding */ HttpCursor)
/* harmony export */ });
/* harmony import */ var _byte_queue_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(4375);
/* harmony import */ var _cursor_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(1815);
/* harmony import */ var _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3806);
/* harmony import */ var _encoding_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(5921);
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(8910);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(5233);
/* harmony import */ var _json_decode_js__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(591);
/* harmony import */ var _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(5760);
/* harmony import */ var _shared_json_decode_js__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(834);
/* harmony import */ var _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(2845);










class HttpCursor extends _cursor_js__WEBPACK_IMPORTED_MODULE_1__/* .Cursor */ .b {
    #stream;
    #encoding;
    #reader;
    #queue;
    #closed;
    #done;
    /** @private */
    constructor(stream, encoding) {
        super();
        this.#stream = stream;
        this.#encoding = encoding;
        this.#reader = undefined;
        this.#queue = new _byte_queue_js__WEBPACK_IMPORTED_MODULE_0__/* .ByteQueue */ .y(16 * 1024);
        this.#closed = undefined;
        this.#done = false;
    }
    async open(response) {
        if (response.body === null) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_4__/* .ProtoError */ .pO("No response body for cursor request");
        }
        this.#reader = response.body.getReader();
        const respBody = await this.#nextItem(_json_decode_js__WEBPACK_IMPORTED_MODULE_6__/* .CursorRespBody */ .d, _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_7__/* .CursorRespBody */ .d);
        if (respBody === undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_4__/* .ProtoError */ .pO("Empty response to cursor request");
        }
        return respBody;
    }
    /** Fetch the next entry from the cursor. */
    next() {
        return this.#nextItem(_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_8__/* .CursorEntry */ .pQ, _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_9__/* .CursorEntry */ .pQ);
    }
    /** Close the cursor. */
    close() {
        this._setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_4__/* .ClientError */ .MZ("Cursor was manually closed"));
    }
    /** @private */
    _setClosed(error) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        this.#stream._cursorClosed(this);
        if (this.#reader !== undefined) {
            this.#reader.cancel();
        }
    }
    /** True if the cursor is closed. */
    get closed() {
        return this.#closed !== undefined;
    }
    async #nextItem(jsonFun, protobufDef) {
        for (;;) {
            if (this.#done) {
                return undefined;
            }
            else if (this.#closed !== undefined) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_4__/* .ClosedError */ .l8("Cursor is closed", this.#closed);
            }
            if (this.#encoding === "json") {
                const jsonData = this.#parseItemJson();
                if (jsonData !== undefined) {
                    const jsonText = new TextDecoder().decode(jsonData);
                    const jsonValue = JSON.parse(jsonText);
                    return _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .readJsonObject */ .op(jsonValue, jsonFun);
                }
            }
            else if (this.#encoding === "protobuf") {
                const protobufData = this.#parseItemProtobuf();
                if (protobufData !== undefined) {
                    return _encoding_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_3__/* .readProtobufMessage */ .B(protobufData, protobufDef);
                }
            }
            else {
                throw (0,_util_js__WEBPACK_IMPORTED_MODULE_5__/* .impossible */ .y)(this.#encoding, "Impossible encoding");
            }
            if (this.#reader === undefined) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_4__/* .InternalError */ .Gd("Attempted to read from HTTP cursor before it was opened");
            }
            const { value, done } = await this.#reader.read();
            if (done && this.#queue.length === 0) {
                this.#done = true;
            }
            else if (done) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_4__/* .ProtoError */ .pO("Unexpected end of cursor stream");
            }
            else {
                this.#queue.push(value);
            }
        }
    }
    #parseItemJson() {
        const data = this.#queue.data();
        const newlineByte = 10;
        const newlinePos = data.indexOf(newlineByte);
        if (newlinePos < 0) {
            return undefined;
        }
        const jsonData = data.slice(0, newlinePos);
        this.#queue.shift(newlinePos + 1);
        return jsonData;
    }
    #parseItemProtobuf() {
        const data = this.#queue.data();
        let varintValue = 0;
        let varintLength = 0;
        for (;;) {
            if (varintLength >= data.byteLength) {
                return undefined;
            }
            const byte = data[varintLength];
            varintValue |= (byte & 0x7f) << (7 * varintLength);
            varintLength += 1;
            if (!(byte & 0x80)) {
                break;
            }
        }
        if (data.byteLength < varintLength + varintValue) {
            return undefined;
        }
        const protobufData = data.slice(varintLength, varintLength + varintValue);
        this.#queue.shift(varintLength + varintValue);
        return protobufData;
    }
}


/***/ },

/***/ 591
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   b: () => (/* binding */ PipelineRespBody),
/* harmony export */   d: () => (/* binding */ CursorRespBody)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3806);
/* harmony import */ var _shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(834);



function PipelineRespBody(obj) {
    const baton = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .stringOpt */ .rJ(obj["baton"]);
    const baseUrl = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .stringOpt */ .rJ(obj["base_url"]);
    const results = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .arrayObjectsMap */ .Ki(obj["results"], StreamResult);
    return { baton, baseUrl, results };
}
function StreamResult(obj) {
    const type = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .string */ .Yj(obj["type"]);
    if (type === "ok") {
        const response = StreamResponse(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["response"]));
        return { type: "ok", response };
    }
    else if (type === "error") {
        const error = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .Error */ .$D)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["error"]));
        return { type: "error", error };
    }
    else {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Unexpected type of StreamResult");
    }
}
function StreamResponse(obj) {
    const type = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .string */ .Yj(obj["type"]);
    if (type === "close") {
        return { type: "close" };
    }
    else if (type === "execute") {
        const result = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .StmtResult */ .Ak)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["result"]));
        return { type: "execute", result };
    }
    else if (type === "batch") {
        const result = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .BatchResult */ .et)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["result"]));
        return { type: "batch", result };
    }
    else if (type === "sequence") {
        return { type: "sequence" };
    }
    else if (type === "describe") {
        const result = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .DescribeResult */ .J4)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["result"]));
        return { type: "describe", result };
    }
    else if (type === "store_sql") {
        return { type: "store_sql" };
    }
    else if (type === "close_sql") {
        return { type: "close_sql" };
    }
    else if (type === "get_autocommit") {
        const isAutocommit = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .boolean */ .zM(obj["is_autocommit"]);
        return { type: "get_autocommit", isAutocommit };
    }
    else {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Unexpected type of StreamResponse");
    }
}
function CursorRespBody(obj) {
    const baton = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .stringOpt */ .rJ(obj["baton"]);
    const baseUrl = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .stringOpt */ .rJ(obj["base_url"]);
    return { baton, baseUrl };
}


/***/ },

/***/ 811
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   B: () => (/* binding */ PipelineReqBody),
/* harmony export */   L: () => (/* binding */ CursorReqBody)
/* harmony export */ });
/* harmony import */ var _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(1958);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5233);


function PipelineReqBody(w, msg) {
    if (msg.baton !== undefined) {
        w.string("baton", msg.baton);
    }
    w.arrayObjects("requests", msg.requests, StreamRequest);
}
function StreamRequest(w, msg) {
    w.stringRaw("type", msg.type);
    if (msg.type === "close") {
        // do nothing
    }
    else if (msg.type === "execute") {
        w.object("stmt", msg.stmt, _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Stmt */ .X);
    }
    else if (msg.type === "batch") {
        w.object("batch", msg.batch, _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
    }
    else if (msg.type === "sequence") {
        if (msg.sql !== undefined) {
            w.string("sql", msg.sql);
        }
        if (msg.sqlId !== undefined) {
            w.number("sql_id", msg.sqlId);
        }
    }
    else if (msg.type === "describe") {
        if (msg.sql !== undefined) {
            w.string("sql", msg.sql);
        }
        if (msg.sqlId !== undefined) {
            w.number("sql_id", msg.sqlId);
        }
    }
    else if (msg.type === "store_sql") {
        w.number("sql_id", msg.sqlId);
        w.string("sql", msg.sql);
    }
    else if (msg.type === "close_sql") {
        w.number("sql_id", msg.sqlId);
    }
    else if (msg.type === "get_autocommit") {
        // do nothing
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of StreamRequest");
    }
}
function CursorReqBody(w, msg) {
    if (msg.baton !== undefined) {
        w.string("baton", msg.baton);
    }
    w.object("batch", msg.batch, _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
}


/***/ },

/***/ 5760
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony import */ var _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(2845);

const PipelineRespBody = {
    default() { return { baton: undefined, baseUrl: undefined, results: [] }; },
    1(r, msg) { msg.baton = r.string(); },
    2(r, msg) { msg.baseUrl = r.string(); },
    3(r, msg) { msg.results.push(r.message(StreamResult)); },
};
const StreamResult = {
    default() { return { type: "none" }; },
    1(r) { return { type: "ok", response: r.message(StreamResponse) }; },
    2(r) { return { type: "error", error: r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .Error */ .$D) }; },
};
const StreamResponse = {
    default() { return { type: "none" }; },
    1(r) { return { type: "close" }; },
    2(r) { return r.message(ExecuteStreamResp); },
    3(r) { return r.message(BatchStreamResp); },
    4(r) { return { type: "sequence" }; },
    5(r) { return r.message(DescribeStreamResp); },
    6(r) { return { type: "store_sql" }; },
    7(r) { return { type: "close_sql" }; },
    8(r) { return r.message(GetAutocommitStreamResp); },
};
const ExecuteStreamResp = {
    default() { return { type: "execute", result: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .StmtResult */ .Ak.default() }; },
    1(r, msg) { msg.result = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .StmtResult */ .Ak); },
};
const BatchStreamResp = {
    default() { return { type: "batch", result: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .BatchResult */ .et.default() }; },
    1(r, msg) { msg.result = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .BatchResult */ .et); },
};
const DescribeStreamResp = {
    default() { return { type: "describe", result: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .DescribeResult */ .J4.default() }; },
    1(r, msg) { msg.result = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .DescribeResult */ .J4); },
};
const GetAutocommitStreamResp = {
    default() { return { type: "get_autocommit", isAutocommit: false }; },
    1(r, msg) { msg.isAutocommit = r.bool(); },
};
const CursorRespBody = {
    default() { return { baton: undefined, baseUrl: undefined }; },
    1(r, msg) { msg.baton = r.string(); },
    2(r, msg) { msg.baseUrl = r.string(); },
};

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "b", 0, /* binding */ PipelineRespBody,
/* harmony export */   "d", 0, /* binding */ CursorRespBody
/* harmony export */ ]);


/***/ },

/***/ 2344
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   B: () => (/* binding */ PipelineReqBody),
/* harmony export */   L: () => (/* binding */ CursorReqBody)
/* harmony export */ });
/* harmony import */ var _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(6325);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5233);


function PipelineReqBody(w, msg) {
    if (msg.baton !== undefined) {
        w.string(1, msg.baton);
    }
    for (const req of msg.requests) {
        w.message(2, req, StreamRequest);
    }
}
function StreamRequest(w, msg) {
    if (msg.type === "close") {
        w.message(1, msg, CloseStreamReq);
    }
    else if (msg.type === "execute") {
        w.message(2, msg, ExecuteStreamReq);
    }
    else if (msg.type === "batch") {
        w.message(3, msg, BatchStreamReq);
    }
    else if (msg.type === "sequence") {
        w.message(4, msg, SequenceStreamReq);
    }
    else if (msg.type === "describe") {
        w.message(5, msg, DescribeStreamReq);
    }
    else if (msg.type === "store_sql") {
        w.message(6, msg, StoreSqlStreamReq);
    }
    else if (msg.type === "close_sql") {
        w.message(7, msg, CloseSqlStreamReq);
    }
    else if (msg.type === "get_autocommit") {
        w.message(8, msg, GetAutocommitStreamReq);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of StreamRequest");
    }
}
function CloseStreamReq(_w, _msg) {
}
function ExecuteStreamReq(w, msg) {
    w.message(1, msg.stmt, _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Stmt */ .X);
}
function BatchStreamReq(w, msg) {
    w.message(1, msg.batch, _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
}
function SequenceStreamReq(w, msg) {
    if (msg.sql !== undefined) {
        w.string(1, msg.sql);
    }
    if (msg.sqlId !== undefined) {
        w.int32(2, msg.sqlId);
    }
}
function DescribeStreamReq(w, msg) {
    if (msg.sql !== undefined) {
        w.string(1, msg.sql);
    }
    if (msg.sqlId !== undefined) {
        w.int32(2, msg.sqlId);
    }
}
function StoreSqlStreamReq(w, msg) {
    w.int32(1, msg.sqlId);
    w.string(2, msg.sql);
}
function CloseSqlStreamReq(w, msg) {
    w.int32(1, msg.sqlId);
}
function GetAutocommitStreamReq(_w, _msg) {
}
function CursorReqBody(w, msg) {
    if (msg.baton !== undefined) {
        w.string(1, msg.baton);
    }
    w.message(2, msg.batch, _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
}


/***/ },

/***/ 8348
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   P: () => (/* binding */ HttpStream)
/* harmony export */ });
/* harmony import */ var _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(451);
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(8910);
/* harmony import */ var _encoding_index_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(7511);
/* harmony import */ var _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(6390);
/* harmony import */ var _queue_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(4870);
/* harmony import */ var _queue_microtask_js__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(2580);
/* harmony import */ var _result_js__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(5464);
/* harmony import */ var _sql_js__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(465);
/* harmony import */ var _stream_js__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(5389);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(5233);
/* harmony import */ var _cursor_js__WEBPACK_IMPORTED_MODULE_10__ = __webpack_require__(8342);
/* harmony import */ var _json_encode_js__WEBPACK_IMPORTED_MODULE_11__ = __webpack_require__(811);
/* harmony import */ var _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_12__ = __webpack_require__(2344);
/* harmony import */ var _json_decode_js__WEBPACK_IMPORTED_MODULE_13__ = __webpack_require__(591);
/* harmony import */ var _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_14__ = __webpack_require__(5760);

















class HttpStream extends _stream_js__WEBPACK_IMPORTED_MODULE_8__/* .Stream */ .Z {
    #client;
    #baseUrl;
    #jwt;
    #fetch;
    #baton;
    #queue;
    #flushing;
    #cursor;
    #closing;
    #closeQueued;
    #closed;
    #sqlIdAlloc;
    /** @private */
    constructor(client, baseUrl, jwt, customFetch) {
        super(client.intMode);
        this.#client = client;
        this.#baseUrl = baseUrl.toString();
        this.#jwt = jwt;
        this.#fetch = customFetch;
        this.#baton = undefined;
        this.#queue = new _queue_js__WEBPACK_IMPORTED_MODULE_4__/* .Queue */ .o();
        this.#flushing = false;
        this.#closing = false;
        this.#closeQueued = false;
        this.#closed = undefined;
        this.#sqlIdAlloc = new _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__/* .IdAlloc */ .d();
    }
    /** Get the {@link HttpClient} object that this stream belongs to. */
    client() {
        return this.#client;
    }
    /** @private */
    _sqlOwner() {
        return this;
    }
    /** Cache a SQL text on the server. */
    storeSql(sql) {
        const sqlId = this.#sqlIdAlloc.alloc();
        this.#sendStreamRequest({ type: "store_sql", sqlId, sql }).then(() => undefined, (error) => this._setClosed(error));
        return new _sql_js__WEBPACK_IMPORTED_MODULE_7__/* .Sql */ .d(this, sqlId);
    }
    /** @private */
    _closeSql(sqlId) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#sendStreamRequest({ type: "close_sql", sqlId }).then(() => this.#sqlIdAlloc.free(sqlId), (error) => this._setClosed(error));
    }
    /** @private */
    _execute(stmt) {
        return this.#sendStreamRequest({ type: "execute", stmt }).then((response) => {
            return response.result;
        });
    }
    /** @private */
    _batch(batch) {
        return this.#sendStreamRequest({ type: "batch", batch }).then((response) => {
            return response.result;
        });
    }
    /** @private */
    _describe(protoSql) {
        return this.#sendStreamRequest({
            type: "describe",
            sql: protoSql.sql,
            sqlId: protoSql.sqlId
        }).then((response) => {
            return response.result;
        });
    }
    /** @private */
    _sequence(protoSql) {
        return this.#sendStreamRequest({
            type: "sequence",
            sql: protoSql.sql,
            sqlId: protoSql.sqlId,
        }).then((_response) => {
            return undefined;
        });
    }
    /** Check whether the SQL connection underlying this stream is in autocommit state (i.e., outside of an
     * explicit transaction). This requires protocol version 3 or higher.
     */
    getAutocommit() {
        this.#client._ensureVersion(3, "getAutocommit()");
        return this.#sendStreamRequest({
            type: "get_autocommit",
        }).then((response) => {
            return response.isAutocommit;
        });
    }
    #sendStreamRequest(request) {
        return new Promise((responseCallback, errorCallback) => {
            this.#pushToQueue({ type: "pipeline", request, responseCallback, errorCallback });
        });
    }
    /** @private */
    _openCursor(batch) {
        return new Promise((cursorCallback, errorCallback) => {
            this.#pushToQueue({ type: "cursor", batch, cursorCallback, errorCallback });
        });
    }
    /** @private */
    _cursorClosed(cursor) {
        if (cursor !== this.#cursor) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .InternalError */ .Gd("Cursor was closed, but it was not associated with the stream");
        }
        this.#cursor = undefined;
        (0,_queue_microtask_js__WEBPACK_IMPORTED_MODULE_5__/* .queueMicrotask */ .b)(() => this.#flushQueue());
    }
    /** Immediately close the stream. */
    close() {
        this._setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ClientError */ .MZ("Stream was manually closed"));
    }
    /** Gracefully close the stream. */
    closeGracefully() {
        this.#closing = true;
        (0,_queue_microtask_js__WEBPACK_IMPORTED_MODULE_5__/* .queueMicrotask */ .b)(() => this.#flushQueue());
    }
    /** True if the stream is closed. */
    get closed() {
        return this.#closed !== undefined || this.#closing;
    }
    /** @private */
    _setClosed(error) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        if (this.#cursor !== undefined) {
            this.#cursor._setClosed(error);
        }
        this.#client._streamClosed(this);
        for (;;) {
            const entry = this.#queue.shift();
            if (entry !== undefined) {
                entry.errorCallback(error);
            }
            else {
                break;
            }
        }
        if ((this.#baton !== undefined || this.#flushing) && !this.#closeQueued) {
            this.#queue.push({
                type: "pipeline",
                request: { type: "close" },
                responseCallback: () => undefined,
                errorCallback: () => undefined,
            });
            this.#closeQueued = true;
            (0,_queue_microtask_js__WEBPACK_IMPORTED_MODULE_5__/* .queueMicrotask */ .b)(() => this.#flushQueue());
        }
    }
    #pushToQueue(entry) {
        if (this.#closed !== undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ClosedError */ .l8("Stream is closed", this.#closed);
        }
        else if (this.#closing) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ClosedError */ .l8("Stream is closing", undefined);
        }
        else {
            this.#queue.push(entry);
            (0,_queue_microtask_js__WEBPACK_IMPORTED_MODULE_5__/* .queueMicrotask */ .b)(() => this.#flushQueue());
        }
    }
    #flushQueue() {
        if (this.#flushing || this.#cursor !== undefined) {
            return;
        }
        if (this.#closing && this.#queue.length === 0) {
            this._setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ClientError */ .MZ("Stream was gracefully closed"));
            return;
        }
        const endpoint = this.#client._endpoint;
        if (endpoint === undefined) {
            this.#client._endpointPromise.then(() => this.#flushQueue(), (error) => this._setClosed(error));
            return;
        }
        const firstEntry = this.#queue.shift();
        if (firstEntry === undefined) {
            return;
        }
        else if (firstEntry.type === "pipeline") {
            const pipeline = [firstEntry];
            for (;;) {
                const entry = this.#queue.first();
                if (entry !== undefined && entry.type === "pipeline") {
                    pipeline.push(entry);
                    this.#queue.shift();
                }
                else if (entry === undefined && this.#closing && !this.#closeQueued) {
                    pipeline.push({
                        type: "pipeline",
                        request: { type: "close" },
                        responseCallback: () => undefined,
                        errorCallback: () => undefined,
                    });
                    this.#closeQueued = true;
                    break;
                }
                else {
                    break;
                }
            }
            this.#flushPipeline(endpoint, pipeline);
        }
        else if (firstEntry.type === "cursor") {
            this.#flushCursor(endpoint, firstEntry);
        }
        else {
            throw (0,_util_js__WEBPACK_IMPORTED_MODULE_9__/* .impossible */ .y)(firstEntry, "Impossible type of QueueEntry");
        }
    }
    #flushPipeline(endpoint, pipeline) {
        this.#flush(() => this.#createPipelineRequest(pipeline, endpoint), (resp) => decodePipelineResponse(resp, endpoint.encoding), (respBody) => respBody.baton, (respBody) => respBody.baseUrl, (respBody) => handlePipelineResponse(pipeline, respBody), (error) => pipeline.forEach((entry) => entry.errorCallback(error)));
    }
    #flushCursor(endpoint, entry) {
        const cursor = new _cursor_js__WEBPACK_IMPORTED_MODULE_10__/* .HttpCursor */ .h(this, endpoint.encoding);
        this.#cursor = cursor;
        this.#flush(() => this.#createCursorRequest(entry, endpoint), (resp) => cursor.open(resp), (respBody) => respBody.baton, (respBody) => respBody.baseUrl, (_respBody) => entry.cursorCallback(cursor), (error) => entry.errorCallback(error));
    }
    #flush(createRequest, decodeResponse, getBaton, getBaseUrl, handleResponse, handleError) {
        let promise;
        try {
            const request = createRequest();
            const fetch = this.#fetch;
            promise = fetch(request);
        }
        catch (error) {
            promise = Promise.reject(error);
        }
        this.#flushing = true;
        promise.then((resp) => {
            if (!resp.ok) {
                return errorFromResponse(resp).then((error) => {
                    throw error;
                });
            }
            return decodeResponse(resp);
        }).then((r) => {
            this.#baton = getBaton(r);
            this.#baseUrl = getBaseUrl(r) ?? this.#baseUrl;
            handleResponse(r);
        }).catch((error) => {
            this._setClosed(error);
            handleError(error);
        }).finally(() => {
            this.#flushing = false;
            this.#flushQueue();
        });
    }
    #createPipelineRequest(pipeline, endpoint) {
        return this.#createRequest(new URL(endpoint.pipelinePath, this.#baseUrl), {
            baton: this.#baton,
            requests: pipeline.map((entry) => entry.request),
        }, endpoint.encoding, _json_encode_js__WEBPACK_IMPORTED_MODULE_11__/* .PipelineReqBody */ .B, _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_12__/* .PipelineReqBody */ .B);
    }
    #createCursorRequest(entry, endpoint) {
        if (endpoint.cursorPath === undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ProtocolVersionError */ .XE("Cursors are supported only on protocol version 3 and higher, " +
                `but the HTTP server only supports version ${endpoint.version}.`);
        }
        return this.#createRequest(new URL(endpoint.cursorPath, this.#baseUrl), {
            baton: this.#baton,
            batch: entry.batch,
        }, endpoint.encoding, _json_encode_js__WEBPACK_IMPORTED_MODULE_11__/* .CursorReqBody */ .L, _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_12__/* .CursorReqBody */ .L);
    }
    #createRequest(url, reqBody, encoding, jsonFun, protobufFun) {
        let bodyData;
        let contentType;
        if (encoding === "json") {
            bodyData = (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_2__/* .writeJsonObject */ .jL)(reqBody, jsonFun);
            contentType = "application/json";
        }
        else if (encoding === "protobuf") {
            bodyData = (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_2__/* .writeProtobufMessage */ .s_)(reqBody, protobufFun);
            contentType = "application/x-protobuf";
        }
        else {
            throw (0,_util_js__WEBPACK_IMPORTED_MODULE_9__/* .impossible */ .y)(encoding, "Impossible encoding");
        }
        const headers = new _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_0__/* .Headers */ .Lr();
        headers.set("content-type", contentType);
        if (this.#jwt !== undefined) {
            headers.set("authorization", `Bearer ${this.#jwt}`);
        }
        return new _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_0__/* .Request */ .Kd(url.toString(), { method: "POST", headers, body: bodyData });
    }
}
function handlePipelineResponse(pipeline, respBody) {
    if (respBody.results.length !== pipeline.length) {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ProtoError */ .pO("Server returned unexpected number of pipeline results");
    }
    for (let i = 0; i < pipeline.length; ++i) {
        const result = respBody.results[i];
        const entry = pipeline[i];
        if (result.type === "ok") {
            if (result.response.type !== entry.request.type) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ProtoError */ .pO("Received unexpected type of response");
            }
            entry.responseCallback(result.response);
        }
        else if (result.type === "error") {
            entry.errorCallback((0,_result_js__WEBPACK_IMPORTED_MODULE_6__/* .errorFromProto */ .NC)(result.error));
        }
        else if (result.type === "none") {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ProtoError */ .pO("Received unrecognized type of StreamResult");
        }
        else {
            throw (0,_util_js__WEBPACK_IMPORTED_MODULE_9__/* .impossible */ .y)(result, "Received impossible type of StreamResult");
        }
    }
}
async function decodePipelineResponse(resp, encoding) {
    if (encoding === "json") {
        const respJson = await resp.json();
        return (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_2__/* .readJsonObject */ .op)(respJson, _json_decode_js__WEBPACK_IMPORTED_MODULE_13__/* .PipelineRespBody */ .b);
    }
    else if (encoding === "protobuf") {
        const respData = await resp.arrayBuffer();
        return (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_2__/* .readProtobufMessage */ .BM)(new Uint8Array(respData), _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_14__/* .PipelineRespBody */ .b);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_9__/* .impossible */ .y)(encoding, "Impossible encoding");
    }
}
async function errorFromResponse(resp) {
    const respType = resp.headers.get("content-type") ?? "text/plain";
    if (respType === "application/json") {
        const respBody = await resp.json();
        if ("message" in respBody) {
            return (0,_result_js__WEBPACK_IMPORTED_MODULE_6__/* .errorFromProto */ .NC)(respBody);
        }
    }
    let message = `Server returned HTTP status ${resp.status}`;
    if (respType === "text/plain") {
        const respBody = (await resp.text()).trim();
        if (respBody !== "") {
            message += `: ${respBody}`;
        }
    }
    return new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .HttpServerError */ .Qt(message, resp.status);
}


/***/ },

/***/ 6390
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   d: () => (/* binding */ IdAlloc)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);

// An allocator of non-negative integer ids.
//
// This clever data structure has these "ideal" properties:
// - It consumes memory proportional to the number of used ids (which is optimal).
// - All operations are O(1) time.
// - The allocated ids are small (with a slight modification, we could always provide the smallest possible
// id).
class IdAlloc {
    // Set of all allocated ids
    #usedIds;
    // Set of all free ids lower than `#usedIds.size`
    #freeIds;
    constructor() {
        this.#usedIds = new Set();
        this.#freeIds = new Set();
    }
    // Returns an id that was free, and marks it as used.
    alloc() {
        // this "loop" is just a way to pick an arbitrary element from the `#freeIds` set
        for (const freeId of this.#freeIds) {
            this.#freeIds.delete(freeId);
            this.#usedIds.add(freeId);
            // maintain the invariant of `#freeIds`
            if (!this.#usedIds.has(this.#usedIds.size - 1)) {
                this.#freeIds.add(this.#usedIds.size - 1);
            }
            return freeId;
        }
        // the `#freeIds` set is empty, so there are no free ids lower than `#usedIds.size`
        // this means that `#usedIds` is a set that contains all numbers from 0 to `#usedIds.size - 1`,
        // so `#usedIds.size` is free
        const freeId = this.#usedIds.size;
        this.#usedIds.add(freeId);
        return freeId;
    }
    free(id) {
        if (!this.#usedIds.delete(id)) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .InternalError */ .Gd("Freeing an id that is not allocated");
        }
        // maintain the invariant of `#freeIds`
        this.#freeIds.delete(this.#usedIds.size);
        if (id < this.#usedIds.size) {
            this.#freeIds.add(id);
        }
    }
}


/***/ },

/***/ 5113
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Gd: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.Gd),
/* harmony export */   L8: () => (/* reexport safe */ _batch_js__WEBPACK_IMPORTED_MODULE_5__.L8),
/* harmony export */   MZ: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.MZ),
/* harmony export */   Nb: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.Nb),
/* harmony export */   Qt: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.Qt),
/* harmony export */   XE: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.XE),
/* harmony export */   XO: () => (/* reexport safe */ _stmt_js__WEBPACK_IMPORTED_MODULE_8__.X),
/* harmony export */   l8: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.l8),
/* harmony export */   m5: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.m5),
/* harmony export */   o3: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.o3),
/* harmony export */   pO: () => (/* reexport safe */ _errors_js__WEBPACK_IMPORTED_MODULE_2__.pO),
/* harmony export */   z8: () => (/* binding */ openWs)
/* harmony export */ });
/* unused harmony export openHttp */
/* unused harmony import specifier */ var HttpClient;
/* harmony import */ var _libsql_isomorphic_ws__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(6346);
/* harmony import */ var _ws_client_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(7773);
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(8910);
/* harmony import */ var _http_client_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(1911);
/* harmony import */ var _libsql_isomorphic_fetch__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(451);
/* harmony import */ var _batch_js__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(2801);
/* harmony import */ var _libsql_url_js__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(1030);
/* harmony import */ var _sql_js__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(465);
/* harmony import */ var _stmt_js__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(2219);
/* harmony import */ var _stream_js__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(5389);
/* harmony import */ var _http_stream_js__WEBPACK_IMPORTED_MODULE_10__ = __webpack_require__(8348);
/* harmony import */ var _ws_stream_js__WEBPACK_IMPORTED_MODULE_11__ = __webpack_require__(2570);


















/** Open a Hrana client over WebSocket connected to the given `url`. */
function openWs(url, jwt, protocolVersion = 2) {
    if (typeof _libsql_isomorphic_ws__WEBPACK_IMPORTED_MODULE_0__/* .WebSocket */ .k === "undefined") {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .WebSocketUnsupportedError */ .Nb("WebSockets are not supported in this environment");
    }
    var subprotocols = undefined;
    if (protocolVersion == 3) {
        subprotocols = Array.from(_ws_client_js__WEBPACK_IMPORTED_MODULE_1__/* .subprotocolsV3 */ .xq.keys());
    }
    else {
        subprotocols = Array.from(_ws_client_js__WEBPACK_IMPORTED_MODULE_1__/* .subprotocolsV2 */ .E4.keys());
    }
    const socket = new _libsql_isomorphic_ws__WEBPACK_IMPORTED_MODULE_0__/* .WebSocket */ .k(url, subprotocols);
    return new _ws_client_js__WEBPACK_IMPORTED_MODULE_1__/* .WsClient */ .I9(socket, jwt);
}
/** Open a Hrana client over HTTP connected to the given `url`.
 *
 * If the `customFetch` argument is passed and not `undefined`, it is used in place of the `fetch` function
 * from `@libsql/isomorphic-fetch`. This function is always called with a `Request` object from
 * `@libsql/isomorphic-fetch`.
 */
function openHttp(url, jwt, customFetch, protocolVersion = 2) {
    return new HttpClient(url instanceof URL ? url : new URL(url), jwt, customFetch, protocolVersion);
}


/***/ },

/***/ 1030
(__unused_webpack_module, __unused_webpack___webpack_exports__, __webpack_require__) {

/* unused harmony export parseLibsqlUrl */
/* unused harmony import specifier */ var LibsqlUrlParseError;
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);

;
/** Parses a URL compatible with the libsql client (`@libsql/client`). This URL may have the "libsql:" scheme
 * and may contain query parameters. */
function parseLibsqlUrl(urlStr) {
    const url = new URL(urlStr);
    let authToken = undefined;
    let tls = undefined;
    for (const [key, value] of url.searchParams.entries()) {
        if (key === "authToken") {
            authToken = value;
        }
        else if (key === "tls") {
            if (value === "0") {
                tls = false;
            }
            else if (value === "1") {
                tls = true;
            }
            else {
                throw new LibsqlUrlParseError(`Unknown value for the "tls" query argument: ${JSON.stringify(value)}`);
            }
        }
        else {
            throw new LibsqlUrlParseError(`Unknown URL query argument ${JSON.stringify(key)}`);
        }
    }
    let hranaWsScheme;
    let hranaHttpScheme;
    if ((url.protocol === "http:" || url.protocol === "ws:") && (tls === true)) {
        throw new LibsqlUrlParseError(`A ${JSON.stringify(url.protocol)} URL cannot opt into TLS using ?tls=1`);
    }
    else if ((url.protocol === "https:" || url.protocol === "wss:") && (tls === false)) {
        throw new LibsqlUrlParseError(`A ${JSON.stringify(url.protocol)} URL cannot opt out of TLS using ?tls=0`);
    }
    if (url.protocol === "http:" || url.protocol === "https:") {
        hranaHttpScheme = url.protocol;
    }
    else if (url.protocol === "ws:" || url.protocol === "wss:") {
        hranaWsScheme = url.protocol;
    }
    else if (url.protocol === "libsql:") {
        if (tls === false) {
            if (!url.port) {
                throw new LibsqlUrlParseError(`A "libsql:" URL with ?tls=0 must specify an explicit port`);
            }
            hranaHttpScheme = "http:";
            hranaWsScheme = "ws:";
        }
        else {
            hranaHttpScheme = "https:";
            hranaWsScheme = "wss:";
        }
    }
    else {
        throw new LibsqlUrlParseError(`This client does not support ${JSON.stringify(url.protocol)} URLs. ` +
            'Please use a "libsql:", "ws:", "wss:", "http:" or "https:" URL instead.');
    }
    if (url.username || url.password) {
        throw new LibsqlUrlParseError("This client does not support HTTP Basic authentication with a username and password. " +
            'You can authenticate using a token passed in the "authToken" URL query parameter.');
    }
    if (url.hash) {
        throw new LibsqlUrlParseError("URL fragments are not supported");
    }
    let hranaPath = url.pathname;
    if (hranaPath === "/") {
        hranaPath = "";
    }
    const hranaWsUrl = hranaWsScheme !== undefined
        ? `${hranaWsScheme}//${url.host}${hranaPath}` : undefined;
    const hranaHttpUrl = hranaHttpScheme !== undefined
        ? `${hranaHttpScheme}//${url.host}${hranaPath}` : undefined;
    return { hranaWsUrl, hranaHttpUrl, authToken };
}


/***/ },

/***/ 4870
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   o: () => (/* binding */ Queue)
/* harmony export */ });
class Queue {
    #pushStack;
    #shiftStack;
    constructor() {
        this.#pushStack = [];
        this.#shiftStack = [];
    }
    get length() {
        return this.#pushStack.length + this.#shiftStack.length;
    }
    push(elem) {
        this.#pushStack.push(elem);
    }
    shift() {
        if (this.#shiftStack.length === 0 && this.#pushStack.length > 0) {
            this.#shiftStack = this.#pushStack.reverse();
            this.#pushStack = [];
        }
        return this.#shiftStack.pop();
    }
    first() {
        return this.#shiftStack.length !== 0
            ? this.#shiftStack[this.#shiftStack.length - 1]
            : this.#pushStack[0];
    }
}


/***/ },

/***/ 2580
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   b: () => (/* binding */ _queueMicrotask)
/* harmony export */ });
// queueMicrotask() ponyfill
// https://github.com/libsql/libsql-client-ts/issues/47
let _queueMicrotask;
if (typeof queueMicrotask !== "undefined") {
    _queueMicrotask = queueMicrotask;
}
else {
    const resolved = Promise.resolve();
    _queueMicrotask = (callback) => {
        resolved.then(callback);
    };
}



/***/ },

/***/ 5464
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   NC: () => (/* binding */ errorFromProto),
/* harmony export */   SC: () => (/* binding */ rowResultFromProto),
/* harmony export */   j8: () => (/* binding */ valueResultFromProto),
/* harmony export */   sR: () => (/* binding */ stmtResultFromProto),
/* harmony export */   tc: () => (/* binding */ rowsResultFromProto)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _value_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(752);


function stmtResultFromProto(result) {
    return {
        affectedRowCount: result.affectedRowCount,
        lastInsertRowid: result.lastInsertRowid,
        columnNames: result.cols.map(col => col.name),
        columnDecltypes: result.cols.map(col => col.decltype),
    };
}
function rowsResultFromProto(result, intMode) {
    const stmtResult = stmtResultFromProto(result);
    const rows = result.rows.map(row => rowFromProto(stmtResult.columnNames, row, intMode));
    return { ...stmtResult, rows };
}
function rowResultFromProto(result, intMode) {
    const stmtResult = stmtResultFromProto(result);
    let row;
    if (result.rows.length > 0) {
        row = rowFromProto(stmtResult.columnNames, result.rows[0], intMode);
    }
    return { ...stmtResult, row };
}
function valueResultFromProto(result, intMode) {
    const stmtResult = stmtResultFromProto(result);
    let value;
    if (result.rows.length > 0 && stmtResult.columnNames.length > 0) {
        value = (0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueFromProto */ .G)(result.rows[0][0], intMode);
    }
    return { ...stmtResult, value };
}
function rowFromProto(colNames, values, intMode) {
    const row = {};
    // make sure that the "length" property is not enumerable
    Object.defineProperty(row, "length", { value: values.length });
    for (let i = 0; i < values.length; ++i) {
        const value = (0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueFromProto */ .G)(values[i], intMode);
        Object.defineProperty(row, i, { value });
        const colName = colNames[i];
        if (colName !== undefined && !Object.hasOwn(row, colName)) {
            Object.defineProperty(row, colName, { value, enumerable: true, configurable: true, writable: true });
        }
    }
    return row;
}
function errorFromProto(error) {
    return new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ResponseError */ .o3(error.message, error);
}


/***/ },

/***/ 834
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   $D: () => (/* binding */ Error),
/* harmony export */   Ak: () => (/* binding */ StmtResult),
/* harmony export */   J4: () => (/* binding */ DescribeResult),
/* harmony export */   et: () => (/* binding */ BatchResult),
/* harmony export */   pQ: () => (/* binding */ CursorEntry)
/* harmony export */ });
/* unused harmony export Value */
/* harmony import */ var js_base64__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(18);
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(8910);
/* harmony import */ var _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(3806);



function Error(obj) {
    const message = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["message"]);
    const code = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["code"]);
    return { message, code };
}
function StmtResult(obj) {
    const cols = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .arrayObjectsMap */ .Ki(obj["cols"], Col);
    const rows = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .array */ .YO(obj["rows"]).map((rowObj) => _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .arrayObjectsMap */ .Ki(rowObj, Value));
    const affectedRowCount = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .number */ .ai(obj["affected_row_count"]);
    const lastInsertRowidStr = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["last_insert_rowid"]);
    const lastInsertRowid = lastInsertRowidStr !== undefined
        ? BigInt(lastInsertRowidStr) : undefined;
    return { cols, rows, affectedRowCount, lastInsertRowid };
}
function Col(obj) {
    const name = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["name"]);
    const decltype = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["decltype"]);
    return { name, decltype };
}
function BatchResult(obj) {
    const stepResults = new Map();
    _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .array */ .YO(obj["step_results"]).forEach((value, i) => {
        if (value !== null) {
            stepResults.set(i, StmtResult(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .object */ .Ik(value)));
        }
    });
    const stepErrors = new Map();
    _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .array */ .YO(obj["step_errors"]).forEach((value, i) => {
        if (value !== null) {
            stepErrors.set(i, Error(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .object */ .Ik(value)));
        }
    });
    return { stepResults, stepErrors };
}
function CursorEntry(obj) {
    const type = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["type"]);
    if (type === "step_begin") {
        const step = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .number */ .ai(obj["step"]);
        const cols = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .arrayObjectsMap */ .Ki(obj["cols"], Col);
        return { type: "step_begin", step, cols };
    }
    else if (type === "step_end") {
        const affectedRowCount = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .number */ .ai(obj["affected_row_count"]);
        const lastInsertRowidStr = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["last_insert_rowid"]);
        const lastInsertRowid = lastInsertRowidStr !== undefined
            ? BigInt(lastInsertRowidStr) : undefined;
        return { type: "step_end", affectedRowCount, lastInsertRowid };
    }
    else if (type === "step_error") {
        const step = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .number */ .ai(obj["step"]);
        const error = Error(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .object */ .Ik(obj["error"]));
        return { type: "step_error", step, error };
    }
    else if (type === "row") {
        const row = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .arrayObjectsMap */ .Ki(obj["row"], Value);
        return { type: "row", row };
    }
    else if (type === "error") {
        const error = Error(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .object */ .Ik(obj["error"]));
        return { type: "error", error };
    }
    else {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ProtoError */ .pO("Unexpected type of CursorEntry");
    }
}
function DescribeResult(obj) {
    const params = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .arrayObjectsMap */ .Ki(obj["params"], DescribeParam);
    const cols = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .arrayObjectsMap */ .Ki(obj["cols"], DescribeCol);
    const isExplain = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .boolean */ .zM(obj["is_explain"]);
    const isReadonly = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .boolean */ .zM(obj["is_readonly"]);
    return { params, cols, isExplain, isReadonly };
}
function DescribeParam(obj) {
    const name = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["name"]);
    return { name };
}
function DescribeCol(obj) {
    const name = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["name"]);
    const decltype = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .stringOpt */ .rJ(obj["decltype"]);
    return { name, decltype };
}
function Value(obj) {
    const type = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["type"]);
    if (type === "null") {
        return null;
    }
    else if (type === "integer") {
        const value = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["value"]);
        return BigInt(value);
    }
    else if (type === "float") {
        return _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .number */ .ai(obj["value"]);
    }
    else if (type === "text") {
        return _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["value"]);
    }
    else if (type === "blob") {
        return js_base64__WEBPACK_IMPORTED_MODULE_0__/* .Base64 */ .o4.toUint8Array(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .string */ .Yj(obj["base64"]));
    }
    else {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_1__/* .ProtoError */ .pO("Unexpected type of Value");
    }
}


/***/ },

/***/ 1958
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   X: () => (/* binding */ Stmt),
/* harmony export */   l: () => (/* binding */ Batch)
/* harmony export */ });
/* harmony import */ var js_base64__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(18);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5233);


function Stmt(w, msg) {
    if (msg.sql !== undefined) {
        w.string("sql", msg.sql);
    }
    if (msg.sqlId !== undefined) {
        w.number("sql_id", msg.sqlId);
    }
    w.arrayObjects("args", msg.args, Value);
    w.arrayObjects("named_args", msg.namedArgs, NamedArg);
    w.boolean("want_rows", msg.wantRows);
}
function NamedArg(w, msg) {
    w.string("name", msg.name);
    w.object("value", msg.value, Value);
}
function Batch(w, msg) {
    w.arrayObjects("steps", msg.steps, BatchStep);
}
function BatchStep(w, msg) {
    if (msg.condition !== undefined) {
        w.object("condition", msg.condition, BatchCond);
    }
    w.object("stmt", msg.stmt, Stmt);
}
function BatchCond(w, msg) {
    w.stringRaw("type", msg.type);
    if (msg.type === "ok" || msg.type === "error") {
        w.number("step", msg.step);
    }
    else if (msg.type === "not") {
        w.object("cond", msg.cond, BatchCond);
    }
    else if (msg.type === "and" || msg.type === "or") {
        w.arrayObjects("conds", msg.conds, BatchCond);
    }
    else if (msg.type === "is_autocommit") {
        // do nothing
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of BatchCond");
    }
}
function Value(w, msg) {
    if (msg === null) {
        w.stringRaw("type", "null");
    }
    else if (typeof msg === "bigint") {
        w.stringRaw("type", "integer");
        w.stringRaw("value", "" + msg);
    }
    else if (typeof msg === "number") {
        w.stringRaw("type", "float");
        w.number("value", msg);
    }
    else if (typeof msg === "string") {
        w.stringRaw("type", "text");
        w.string("value", msg);
    }
    else if (msg instanceof Uint8Array) {
        w.stringRaw("type", "blob");
        w.stringRaw("base64", js_base64__WEBPACK_IMPORTED_MODULE_0__/* .Base64 */ .o4.fromUint8Array(msg));
    }
    else if (msg === undefined) {
        // do nothing
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of Value");
    }
}


/***/ },

/***/ 2845
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

const Error = {
    default() { return { message: "", code: undefined }; },
    1(r, msg) { msg.message = r.string(); },
    2(r, msg) { msg.code = r.string(); },
};
const StmtResult = {
    default() {
        return {
            cols: [],
            rows: [],
            affectedRowCount: 0,
            lastInsertRowid: undefined,
        };
    },
    1(r, msg) { msg.cols.push(r.message(Col)); },
    2(r, msg) { msg.rows.push(r.message(Row)); },
    3(r, msg) { msg.affectedRowCount = Number(r.uint64()); },
    4(r, msg) { msg.lastInsertRowid = r.sint64(); },
};
const Col = {
    default() { return { name: undefined, decltype: undefined }; },
    1(r, msg) { msg.name = r.string(); },
    2(r, msg) { msg.decltype = r.string(); },
};
const Row = {
    default() { return []; },
    1(r, msg) { msg.push(r.message(Value)); },
};
const BatchResult = {
    default() { return { stepResults: new Map(), stepErrors: new Map() }; },
    1(r, msg) {
        const [key, value] = r.message(BatchResultStepResult);
        msg.stepResults.set(key, value);
    },
    2(r, msg) {
        const [key, value] = r.message(BatchResultStepError);
        msg.stepErrors.set(key, value);
    },
};
const BatchResultStepResult = {
    default() { return [0, StmtResult.default()]; },
    1(r, msg) { msg[0] = r.uint32(); },
    2(r, msg) { msg[1] = r.message(StmtResult); },
};
const BatchResultStepError = {
    default() { return [0, Error.default()]; },
    1(r, msg) { msg[0] = r.uint32(); },
    2(r, msg) { msg[1] = r.message(Error); },
};
const CursorEntry = {
    default() { return { type: "none" }; },
    1(r) { return r.message(StepBeginEntry); },
    2(r) { return r.message(StepEndEntry); },
    3(r) { return r.message(StepErrorEntry); },
    4(r) { return { type: "row", row: r.message(Row) }; },
    5(r) { return { type: "error", error: r.message(Error) }; },
};
const StepBeginEntry = {
    default() { return { type: "step_begin", step: 0, cols: [] }; },
    1(r, msg) { msg.step = r.uint32(); },
    2(r, msg) { msg.cols.push(r.message(Col)); },
};
const StepEndEntry = {
    default() {
        return {
            type: "step_end",
            affectedRowCount: 0,
            lastInsertRowid: undefined,
        };
    },
    1(r, msg) { msg.affectedRowCount = r.uint32(); },
    2(r, msg) { msg.lastInsertRowid = r.uint64(); },
};
const StepErrorEntry = {
    default() {
        return {
            type: "step_error",
            step: 0,
            error: Error.default(),
        };
    },
    1(r, msg) { msg.step = r.uint32(); },
    2(r, msg) { msg.error = r.message(Error); },
};
const DescribeResult = {
    default() {
        return {
            params: [],
            cols: [],
            isExplain: false,
            isReadonly: false,
        };
    },
    1(r, msg) { msg.params.push(r.message(DescribeParam)); },
    2(r, msg) { msg.cols.push(r.message(DescribeCol)); },
    3(r, msg) { msg.isExplain = r.bool(); },
    4(r, msg) { msg.isReadonly = r.bool(); },
};
const DescribeParam = {
    default() { return { name: undefined }; },
    1(r, msg) { msg.name = r.string(); },
};
const DescribeCol = {
    default() { return { name: "", decltype: undefined }; },
    1(r, msg) { msg.name = r.string(); },
    2(r, msg) { msg.decltype = r.string(); },
};
const Value = {
    default() { return undefined; },
    1(r) { return null; },
    2(r) { return r.sint64(); },
    3(r) { return r.double(); },
    4(r) { return r.string(); },
    5(r) { return r.bytes(); },
};

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "$D", 0, /* binding */ Error,
/* harmony export */   "Ak", 0, /* binding */ StmtResult,
/* harmony export */   "J4", 0, /* binding */ DescribeResult,
/* harmony export */   "et", 0, /* binding */ BatchResult,
/* harmony export */   "pQ", 0, /* binding */ CursorEntry
/* harmony export */ ]);


/***/ },

/***/ 6325
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   X: () => (/* binding */ Stmt),
/* harmony export */   l: () => (/* binding */ Batch)
/* harmony export */ });
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(5233);

function Stmt(w, msg) {
    if (msg.sql !== undefined) {
        w.string(1, msg.sql);
    }
    if (msg.sqlId !== undefined) {
        w.int32(2, msg.sqlId);
    }
    for (const arg of msg.args) {
        w.message(3, arg, Value);
    }
    for (const arg of msg.namedArgs) {
        w.message(4, arg, NamedArg);
    }
    w.bool(5, msg.wantRows);
}
function NamedArg(w, msg) {
    w.string(1, msg.name);
    w.message(2, msg.value, Value);
}
function Batch(w, msg) {
    for (const step of msg.steps) {
        w.message(1, step, BatchStep);
    }
}
function BatchStep(w, msg) {
    if (msg.condition !== undefined) {
        w.message(1, msg.condition, BatchCond);
    }
    w.message(2, msg.stmt, Stmt);
}
function BatchCond(w, msg) {
    if (msg.type === "ok") {
        w.uint32(1, msg.step);
    }
    else if (msg.type === "error") {
        w.uint32(2, msg.step);
    }
    else if (msg.type === "not") {
        w.message(3, msg.cond, BatchCond);
    }
    else if (msg.type === "and") {
        w.message(4, msg.conds, BatchCondList);
    }
    else if (msg.type === "or") {
        w.message(5, msg.conds, BatchCondList);
    }
    else if (msg.type === "is_autocommit") {
        w.message(6, undefined, Empty);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_0__/* .impossible */ .y)(msg, "Impossible type of BatchCond");
    }
}
function BatchCondList(w, msg) {
    for (const cond of msg) {
        w.message(1, cond, BatchCond);
    }
}
function Value(w, msg) {
    if (msg === null) {
        w.message(1, undefined, Empty);
    }
    else if (typeof msg === "bigint") {
        w.sint64(2, msg);
    }
    else if (typeof msg === "number") {
        w.double(3, msg);
    }
    else if (typeof msg === "string") {
        w.string(4, msg);
    }
    else if (msg instanceof Uint8Array) {
        w.bytes(5, msg);
    }
    else if (msg === undefined) {
        // do nothing
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_0__/* .impossible */ .y)(msg, "Impossible type of Value");
    }
}
function Empty(_w, _msg) {
    // do nothing
}


/***/ },

/***/ 465
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   d: () => (/* binding */ Sql),
/* harmony export */   y: () => (/* binding */ sqlToProto)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);

/** Text of an SQL statement cached on the server. */
class Sql {
    #owner;
    #sqlId;
    #closed;
    /** @private */
    constructor(owner, sqlId) {
        this.#owner = owner;
        this.#sqlId = sqlId;
        this.#closed = undefined;
    }
    /** @private */
    _getSqlId(owner) {
        if (this.#owner !== owner) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .MisuseError */ .Xq("Attempted to use SQL text opened with other object");
        }
        else if (this.#closed !== undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClosedError */ .l8("SQL text is closed", this.#closed);
        }
        return this.#sqlId;
    }
    /** Remove the SQL text from the server, releasing resouces. */
    close() {
        this._setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClientError */ .MZ("SQL text was manually closed"));
    }
    /** @private */
    _setClosed(error) {
        if (this.#closed === undefined) {
            this.#closed = error;
            this.#owner._closeSql(this.#sqlId);
        }
    }
    /** True if the SQL text is closed (removed from the server). */
    get closed() {
        return this.#closed !== undefined;
    }
}
function sqlToProto(owner, sql) {
    if (sql instanceof Sql) {
        return { sqlId: sql._getSqlId(owner) };
    }
    else {
        return { sql: "" + sql };
    }
}


/***/ },

/***/ 2219
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   O: () => (/* binding */ stmtToProto),
/* harmony export */   X: () => (/* binding */ Stmt)
/* harmony export */ });
/* harmony import */ var _sql_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(465);
/* harmony import */ var _value_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(752);


/** A statement that can be evaluated by the database. Besides the SQL text, it also contains the positional
 * and named arguments. */
class Stmt {
    /** The SQL statement text. */
    sql;
    /** @private */
    _args;
    /** @private */
    _namedArgs;
    /** Initialize the statement with given SQL text. */
    constructor(sql) {
        this.sql = sql;
        this._args = [];
        this._namedArgs = new Map();
    }
    /** Binds positional parameters from the given `values`. All previous positional bindings are cleared. */
    bindIndexes(values) {
        this._args.length = 0;
        for (const value of values) {
            this._args.push((0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueToProto */ .n)(value));
        }
        return this;
    }
    /** Binds a parameter by a 1-based index. */
    bindIndex(index, value) {
        if (index !== (index | 0) || index <= 0) {
            throw new RangeError("Index of a positional argument must be positive integer");
        }
        while (this._args.length < index) {
            this._args.push(null);
        }
        this._args[index - 1] = (0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueToProto */ .n)(value);
        return this;
    }
    /** Binds a parameter by name. */
    bindName(name, value) {
        this._namedArgs.set(name, (0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueToProto */ .n)(value));
        return this;
    }
    /** Clears all bindings. */
    unbindAll() {
        this._args.length = 0;
        this._namedArgs.clear();
        return this;
    }
}
function stmtToProto(sqlOwner, stmt, wantRows) {
    let inSql;
    let args = [];
    let namedArgs = [];
    if (stmt instanceof Stmt) {
        inSql = stmt.sql;
        args = stmt._args;
        for (const [name, value] of stmt._namedArgs.entries()) {
            namedArgs.push({ name, value });
        }
    }
    else if (Array.isArray(stmt)) {
        inSql = stmt[0];
        if (Array.isArray(stmt[1])) {
            args = stmt[1].map((arg) => (0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueToProto */ .n)(arg));
        }
        else {
            namedArgs = Object.entries(stmt[1]).map(([name, value]) => {
                return { name, value: (0,_value_js__WEBPACK_IMPORTED_MODULE_1__/* .valueToProto */ .n)(value) };
            });
        }
    }
    else {
        inSql = stmt;
    }
    const { sql, sqlId } = (0,_sql_js__WEBPACK_IMPORTED_MODULE_0__/* .sqlToProto */ .y)(sqlOwner, inSql);
    return { sql, sqlId, args, namedArgs, wantRows };
}


/***/ },

/***/ 5389
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Z: () => (/* binding */ Stream)
/* harmony export */ });
/* harmony import */ var _batch_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(2801);
/* harmony import */ var _describe_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(6264);
/* harmony import */ var _result_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(5464);
/* harmony import */ var _sql_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(465);
/* harmony import */ var _stmt_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(2219);





/** A stream for executing SQL statements (a "database connection"). */
class Stream {
    /** @private */
    constructor(intMode) {
        this.intMode = intMode;
    }
    /** Execute a statement and return rows. */
    query(stmt) {
        return this.#execute(stmt, true, _result_js__WEBPACK_IMPORTED_MODULE_2__/* .rowsResultFromProto */ .tc);
    }
    /** Execute a statement and return at most a single row. */
    queryRow(stmt) {
        return this.#execute(stmt, true, _result_js__WEBPACK_IMPORTED_MODULE_2__/* .rowResultFromProto */ .SC);
    }
    /** Execute a statement and return at most a single value. */
    queryValue(stmt) {
        return this.#execute(stmt, true, _result_js__WEBPACK_IMPORTED_MODULE_2__/* .valueResultFromProto */ .j8);
    }
    /** Execute a statement without returning rows. */
    run(stmt) {
        return this.#execute(stmt, false, _result_js__WEBPACK_IMPORTED_MODULE_2__/* .stmtResultFromProto */ .sR);
    }
    #execute(inStmt, wantRows, fromProto) {
        const stmt = (0,_stmt_js__WEBPACK_IMPORTED_MODULE_4__/* .stmtToProto */ .O)(this._sqlOwner(), inStmt, wantRows);
        return this._execute(stmt).then((r) => fromProto(r, this.intMode));
    }
    /** Return a builder for creating and executing a batch.
     *
     * If `useCursor` is true, the batch will be executed using a Hrana cursor, which will stream results from
     * the server to the client, which consumes less memory on the server. This requires protocol version 3 or
     * higher.
     */
    batch(useCursor = false) {
        return new _batch_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .lP(this, useCursor);
    }
    /** Parse and analyze a statement. This requires protocol version 2 or higher. */
    describe(inSql) {
        const protoSql = (0,_sql_js__WEBPACK_IMPORTED_MODULE_3__/* .sqlToProto */ .y)(this._sqlOwner(), inSql);
        return this._describe(protoSql).then(_describe_js__WEBPACK_IMPORTED_MODULE_1__/* .describeResultFromProto */ .b);
    }
    /** Execute a sequence of statements separated by semicolons. This requires protocol version 2 or higher.
     * */
    sequence(inSql) {
        const protoSql = (0,_sql_js__WEBPACK_IMPORTED_MODULE_3__/* .sqlToProto */ .y)(this._sqlOwner(), inSql);
        return this._sequence(protoSql);
    }
    /** Representation of integers returned from the database. See {@link IntMode}.
     *
     * This value affects the results of all operations on this stream.
     */
    intMode;
}


/***/ },

/***/ 5233
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   y: () => (/* binding */ impossible)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);

function impossible(value, message) {
    throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .InternalError */ .Gd(message);
}


/***/ },

/***/ 752
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   G: () => (/* binding */ valueFromProto),
/* harmony export */   n: () => (/* binding */ valueToProto)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5233);


function valueToProto(value) {
    if (value === null) {
        return null;
    }
    else if (typeof value === "string") {
        return value;
    }
    else if (typeof value === "number") {
        if (!Number.isFinite(value)) {
            throw new RangeError("Only finite numbers (not Infinity or NaN) can be passed as arguments");
        }
        return value;
    }
    else if (typeof value === "bigint") {
        if (value < minInteger || value > maxInteger) {
            throw new RangeError("This bigint value is too large to be represented as a 64-bit integer and passed as argument");
        }
        return value;
    }
    else if (typeof value === "boolean") {
        return value ? 1n : 0n;
    }
    else if (value instanceof ArrayBuffer) {
        return new Uint8Array(value);
    }
    else if (value instanceof Uint8Array) {
        return value;
    }
    else if (value instanceof Date) {
        return +value.valueOf();
    }
    else if (typeof value === "object") {
        return "" + value.toString();
    }
    else {
        throw new TypeError("Unsupported type of value");
    }
}
const minInteger = -9223372036854775808n;
const maxInteger = 9223372036854775807n;
function valueFromProto(value, intMode) {
    if (value === null) {
        return null;
    }
    else if (typeof value === "number") {
        return value;
    }
    else if (typeof value === "string") {
        return value;
    }
    else if (typeof value === "bigint") {
        if (intMode === "number") {
            const num = Number(value);
            if (!Number.isSafeInteger(num)) {
                throw new RangeError("Received integer which is too large to be safely represented as a JavaScript number");
            }
            return num;
        }
        else if (intMode === "bigint") {
            return value;
        }
        else if (intMode === "string") {
            return "" + value;
        }
        else {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .MisuseError */ .Xq("Invalid value for IntMode");
        }
    }
    else if (value instanceof Uint8Array) {
        // TODO: we need to copy data from `Uint8Array` to return an `ArrayBuffer`. Perhaps we should add a
        // `blobMode` parameter, similar to `intMode`, which would allow the user to choose between receiving
        // `ArrayBuffer` (default, convenient) and `Uint8Array` (zero copy)?
        return value.slice().buffer;
    }
    else if (value === undefined) {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Received unrecognized type of Value");
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(value, "Impossible type of Value");
    }
}


/***/ },

/***/ 7773
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   I9: () => (/* binding */ WsClient)
/* harmony export */ });
/* harmony import */ var _client_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(4930);
/* harmony import */ var _encoding_index_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(7511);
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(8910);
/* harmony import */ var _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(6390);
/* harmony import */ var _result_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(5464);
/* harmony import */ var _sql_js__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(465);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(5233);
/* harmony import */ var _stream_js__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(2570);
/* harmony import */ var _json_encode_js__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(3349);
/* harmony import */ var _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_9__ = __webpack_require__(6470);
/* harmony import */ var _json_decode_js__WEBPACK_IMPORTED_MODULE_10__ = __webpack_require__(9869);
/* harmony import */ var _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_11__ = __webpack_require__(5474);












const subprotocolsV2 = new Map([
    ["hrana2", { version: 2, encoding: "json" }],
    ["hrana1", { version: 1, encoding: "json" }],
]);
const subprotocolsV3 = new Map([
    ["hrana3-protobuf", { version: 3, encoding: "protobuf" }],
    ["hrana3", { version: 3, encoding: "json" }],
    ["hrana2", { version: 2, encoding: "json" }],
    ["hrana1", { version: 1, encoding: "json" }],
]);
/** A client for the Hrana protocol over a WebSocket. */
class WsClient extends _client_js__WEBPACK_IMPORTED_MODULE_0__/* .Client */ .K {
    #socket;
    // List of callbacks that we queue until the socket transitions from the CONNECTING to the OPEN state.
    #openCallbacks;
    // Have we already transitioned from CONNECTING to OPEN and fired the callbacks in #openCallbacks?
    #opened;
    // Stores the error that caused us to close the client (and the socket). If we are not closed, this is
    // `undefined`.
    #closed;
    // Have we received a response to our "hello" from the server?
    #recvdHello;
    // Subprotocol negotiated with the server. It is only available after the socket transitions to the OPEN
    // state.
    #subprotocol;
    // Has the `getVersion()` function been called? This is only used to validate that the API is used
    // correctly.
    #getVersionCalled;
    // A map from request id to the responses that we expect to receive from the server.
    #responseMap;
    // An allocator of request ids.
    #requestIdAlloc;
    // An allocator of stream ids.
    /** @private */
    _streamIdAlloc;
    // An allocator of cursor ids.
    /** @private */
    _cursorIdAlloc;
    // An allocator of SQL text ids.
    #sqlIdAlloc;
    /** @private */
    constructor(socket, jwt) {
        super();
        this.#socket = socket;
        this.#openCallbacks = [];
        this.#opened = false;
        this.#closed = undefined;
        this.#recvdHello = false;
        this.#subprotocol = undefined;
        this.#getVersionCalled = false;
        this.#responseMap = new Map();
        this.#requestIdAlloc = new _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__/* .IdAlloc */ .d();
        this._streamIdAlloc = new _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__/* .IdAlloc */ .d();
        this._cursorIdAlloc = new _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__/* .IdAlloc */ .d();
        this.#sqlIdAlloc = new _id_alloc_js__WEBPACK_IMPORTED_MODULE_3__/* .IdAlloc */ .d();
        this.#socket.binaryType = "arraybuffer";
        this.#socket.addEventListener("open", () => this.#onSocketOpen());
        this.#socket.addEventListener("close", (event) => this.#onSocketClose(event));
        this.#socket.addEventListener("error", (event) => this.#onSocketError(event));
        this.#socket.addEventListener("message", (event) => this.#onSocketMessage(event));
        this.#send({ type: "hello", jwt });
    }
    // Send (or enqueue to send) a message to the server.
    #send(msg) {
        if (this.#closed !== undefined) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .InternalError */ .Gd("Trying to send a message on a closed client");
        }
        if (this.#opened) {
            this.#sendToSocket(msg);
        }
        else {
            const openCallback = () => this.#sendToSocket(msg);
            const errorCallback = () => undefined;
            this.#openCallbacks.push({ openCallback, errorCallback });
        }
    }
    // The socket transitioned from CONNECTING to OPEN
    #onSocketOpen() {
        const protocol = this.#socket.protocol;
        if (protocol === undefined) {
            this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ClientError */ .MZ("The `WebSocket.protocol` property is undefined. This most likely means that the WebSocket " +
                "implementation provided by the environment is broken. If you are using Miniflare 2, " +
                "please update to Miniflare 3, which fixes this problem."));
            return;
        }
        else if (protocol === "") {
            this.#subprotocol = { version: 1, encoding: "json" };
        }
        else {
            this.#subprotocol = subprotocolsV3.get(protocol);
            if (this.#subprotocol === undefined) {
                this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO(`Unrecognized WebSocket subprotocol: ${JSON.stringify(protocol)}`));
                return;
            }
        }
        for (const callbacks of this.#openCallbacks) {
            callbacks.openCallback();
        }
        this.#openCallbacks.length = 0;
        this.#opened = true;
    }
    #sendToSocket(msg) {
        const encoding = this.#subprotocol.encoding;
        if (encoding === "json") {
            const jsonMsg = (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_1__/* .writeJsonObject */ .jL)(msg, _json_encode_js__WEBPACK_IMPORTED_MODULE_8__/* .ClientMsg */ .l);
            this.#socket.send(jsonMsg);
        }
        else if (encoding === "protobuf") {
            const protobufMsg = (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_1__/* .writeProtobufMessage */ .s_)(msg, _protobuf_encode_js__WEBPACK_IMPORTED_MODULE_9__/* .ClientMsg */ .l);
            this.#socket.send(protobufMsg);
        }
        else {
            throw (0,_util_js__WEBPACK_IMPORTED_MODULE_6__/* .impossible */ .y)(encoding, "Impossible encoding");
        }
    }
    /** Get the protocol version negotiated with the server, possibly waiting until the socket is open. */
    getVersion() {
        return new Promise((versionCallback, errorCallback) => {
            this.#getVersionCalled = true;
            if (this.#closed !== undefined) {
                errorCallback(this.#closed);
            }
            else if (!this.#opened) {
                const openCallback = () => versionCallback(this.#subprotocol.version);
                this.#openCallbacks.push({ openCallback, errorCallback });
            }
            else {
                versionCallback(this.#subprotocol.version);
            }
        });
    }
    // Make sure that the negotiated version is at least `minVersion`.
    /** @private */
    _ensureVersion(minVersion, feature) {
        if (this.#subprotocol === undefined || !this.#getVersionCalled) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtocolVersionError */ .XE(`${feature} is supported only on protocol version ${minVersion} and higher, ` +
                "but the version supported by the WebSocket server is not yet known. " +
                "Use Client.getVersion() to wait until the version is available.");
        }
        else if (this.#subprotocol.version < minVersion) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtocolVersionError */ .XE(`${feature} is supported on protocol version ${minVersion} and higher, ` +
                `but the WebSocket server only supports version ${this.#subprotocol.version}`);
        }
    }
    // Send a request to the server and invoke a callback when we get the response.
    /** @private */
    _sendRequest(request, callbacks) {
        if (this.#closed !== undefined) {
            callbacks.errorCallback(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ClosedError */ .l8("Client is closed", this.#closed));
            return;
        }
        const requestId = this.#requestIdAlloc.alloc();
        this.#responseMap.set(requestId, { ...callbacks, type: request.type });
        this.#send({ type: "request", requestId, request });
    }
    // The socket encountered an error.
    #onSocketError(event) {
        const eventMessage = event.message;
        const message = eventMessage ?? "WebSocket was closed due to an error";
        this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .WebSocketError */ .m5(message));
    }
    // The socket was closed.
    #onSocketClose(event) {
        let message = `WebSocket was closed with code ${event.code}`;
        if (event.reason) {
            message += `: ${event.reason}`;
        }
        this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .WebSocketError */ .m5(message));
    }
    // Close the client with the given error.
    #setClosed(error) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        for (const callbacks of this.#openCallbacks) {
            callbacks.errorCallback(error);
        }
        this.#openCallbacks.length = 0;
        for (const [requestId, responseState] of this.#responseMap.entries()) {
            responseState.errorCallback(error);
            this.#requestIdAlloc.free(requestId);
        }
        this.#responseMap.clear();
        this.#socket.close();
    }
    // We received a message from the socket.
    #onSocketMessage(event) {
        if (this.#closed !== undefined) {
            return;
        }
        try {
            let msg;
            const encoding = this.#subprotocol.encoding;
            if (encoding === "json") {
                if (typeof event.data !== "string") {
                    this.#socket.close(3003, "Only text messages are accepted with JSON encoding");
                    this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received non-text message from server with JSON encoding"));
                    return;
                }
                msg = (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_1__/* .readJsonObject */ .op)(JSON.parse(event.data), _json_decode_js__WEBPACK_IMPORTED_MODULE_10__/* .ServerMsg */ .V);
            }
            else if (encoding === "protobuf") {
                if (!(event.data instanceof ArrayBuffer)) {
                    this.#socket.close(3003, "Only binary messages are accepted with Protobuf encoding");
                    this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received non-binary message from server with Protobuf encoding"));
                    return;
                }
                msg = (0,_encoding_index_js__WEBPACK_IMPORTED_MODULE_1__/* .readProtobufMessage */ .BM)(new Uint8Array(event.data), _protobuf_decode_js__WEBPACK_IMPORTED_MODULE_11__/* .ServerMsg */ .V);
            }
            else {
                throw (0,_util_js__WEBPACK_IMPORTED_MODULE_6__/* .impossible */ .y)(encoding, "Impossible encoding");
            }
            this.#handleMsg(msg);
        }
        catch (e) {
            this.#socket.close(3007, "Could not handle message");
            this.#setClosed(e);
        }
    }
    // Handle a message from the server.
    #handleMsg(msg) {
        if (msg.type === "none") {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received an unrecognized ServerMsg");
        }
        else if (msg.type === "hello_ok" || msg.type === "hello_error") {
            if (this.#recvdHello) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received a duplicated hello response");
            }
            this.#recvdHello = true;
            if (msg.type === "hello_error") {
                throw (0,_result_js__WEBPACK_IMPORTED_MODULE_4__/* .errorFromProto */ .NC)(msg.error);
            }
            return;
        }
        else if (!this.#recvdHello) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received a non-hello message before a hello response");
        }
        if (msg.type === "response_ok") {
            const requestId = msg.requestId;
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);
            if (responseState === undefined) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received unexpected OK response");
            }
            this.#requestIdAlloc.free(requestId);
            try {
                if (responseState.type !== msg.response.type) {
                    console.dir({ responseState, msg });
                    throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received unexpected type of response");
                }
                responseState.responseCallback(msg.response);
            }
            catch (e) {
                responseState.errorCallback(e);
                throw e;
            }
        }
        else if (msg.type === "response_error") {
            const requestId = msg.requestId;
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);
            if (responseState === undefined) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ProtoError */ .pO("Received unexpected error response");
            }
            this.#requestIdAlloc.free(requestId);
            responseState.errorCallback((0,_result_js__WEBPACK_IMPORTED_MODULE_4__/* .errorFromProto */ .NC)(msg.error));
        }
        else {
            throw (0,_util_js__WEBPACK_IMPORTED_MODULE_6__/* .impossible */ .y)(msg, "Impossible ServerMsg type");
        }
    }
    /** Open a {@link WsStream}, a stream for executing SQL statements. */
    openStream() {
        return _stream_js__WEBPACK_IMPORTED_MODULE_7__/* .WsStream */ .P.open(this);
    }
    /** Cache a SQL text on the server. This requires protocol version 2 or higher. */
    storeSql(sql) {
        this._ensureVersion(2, "storeSql()");
        const sqlId = this.#sqlIdAlloc.alloc();
        const sqlObj = new _sql_js__WEBPACK_IMPORTED_MODULE_5__/* .Sql */ .d(this, sqlId);
        const responseCallback = () => undefined;
        const errorCallback = (e) => sqlObj._setClosed(e);
        const request = { type: "store_sql", sqlId, sql };
        this._sendRequest(request, { responseCallback, errorCallback });
        return sqlObj;
    }
    /** @private */
    _closeSql(sqlId) {
        if (this.#closed !== undefined) {
            return;
        }
        const responseCallback = () => this.#sqlIdAlloc.free(sqlId);
        const errorCallback = (e) => this.#setClosed(e);
        const request = { type: "close_sql", sqlId };
        this._sendRequest(request, { responseCallback, errorCallback });
    }
    /** Close the client and the WebSocket. */
    close() {
        this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_2__/* .ClientError */ .MZ("Client was manually closed"));
    }
    /** True if the client is closed. */
    get closed() {
        return this.#closed !== undefined;
    }
}

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "E4", 0, /* binding */ subprotocolsV2,
/* harmony export */   "xq", 0, /* binding */ subprotocolsV3
/* harmony export */ ]);


/***/ },

/***/ 4048
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   V: () => (/* binding */ WsCursor)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _cursor_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(1815);
/* harmony import */ var _queue_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(4870);



const fetchChunkSize = 1000;
const fetchQueueSize = 10;
class WsCursor extends _cursor_js__WEBPACK_IMPORTED_MODULE_1__/* .Cursor */ .b {
    #client;
    #stream;
    #cursorId;
    #entryQueue;
    #fetchQueue;
    #closed;
    #done;
    /** @private */
    constructor(client, stream, cursorId) {
        super();
        this.#client = client;
        this.#stream = stream;
        this.#cursorId = cursorId;
        this.#entryQueue = new _queue_js__WEBPACK_IMPORTED_MODULE_2__/* .Queue */ .o();
        this.#fetchQueue = new _queue_js__WEBPACK_IMPORTED_MODULE_2__/* .Queue */ .o();
        this.#closed = undefined;
        this.#done = false;
    }
    /** Fetch the next entry from the cursor. */
    async next() {
        for (;;) {
            if (this.#closed !== undefined) {
                throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClosedError */ .l8("Cursor is closed", this.#closed);
            }
            while (!this.#done && this.#fetchQueue.length < fetchQueueSize) {
                this.#fetchQueue.push(this.#fetch());
            }
            const entry = this.#entryQueue.shift();
            if (this.#done || entry !== undefined) {
                return entry;
            }
            // we assume that `Cursor.next()` is never called concurrently
            await this.#fetchQueue.shift().then((response) => {
                if (response === undefined) {
                    return;
                }
                for (const entry of response.entries) {
                    this.#entryQueue.push(entry);
                }
                this.#done ||= response.done;
            });
        }
    }
    #fetch() {
        return this.#stream._sendCursorRequest(this, {
            type: "fetch_cursor",
            cursorId: this.#cursorId,
            maxCount: fetchChunkSize,
        }).then((resp) => resp, (error) => {
            this._setClosed(error);
            return undefined;
        });
    }
    /** @private */
    _setClosed(error) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        this.#stream._sendCursorRequest(this, {
            type: "close_cursor",
            cursorId: this.#cursorId,
        }).catch(() => undefined);
        this.#stream._cursorClosed(this);
    }
    /** Close the cursor. */
    close() {
        this._setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClientError */ .MZ("Cursor was manually closed"));
    }
    /** True if the cursor is closed. */
    get closed() {
        return this.#closed !== undefined;
    }
}


/***/ },

/***/ 9869
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   V: () => (/* binding */ ServerMsg)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3806);
/* harmony import */ var _shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(834);



function ServerMsg(obj) {
    const type = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .string */ .Yj(obj["type"]);
    if (type === "hello_ok") {
        return { type: "hello_ok" };
    }
    else if (type === "hello_error") {
        const error = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .Error */ .$D)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["error"]));
        return { type: "hello_error", error };
    }
    else if (type === "response_ok") {
        const requestId = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .number */ .ai(obj["request_id"]);
        const response = Response(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["response"]));
        return { type: "response_ok", requestId, response };
    }
    else if (type === "response_error") {
        const requestId = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .number */ .ai(obj["request_id"]);
        const error = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .Error */ .$D)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["error"]));
        return { type: "response_error", requestId, error };
    }
    else {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Unexpected type of ServerMsg");
    }
}
function Response(obj) {
    const type = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .string */ .Yj(obj["type"]);
    if (type === "open_stream") {
        return { type: "open_stream" };
    }
    else if (type === "close_stream") {
        return { type: "close_stream" };
    }
    else if (type === "execute") {
        const result = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .StmtResult */ .Ak)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["result"]));
        return { type: "execute", result };
    }
    else if (type === "batch") {
        const result = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .BatchResult */ .et)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["result"]));
        return { type: "batch", result };
    }
    else if (type === "open_cursor") {
        return { type: "open_cursor" };
    }
    else if (type === "close_cursor") {
        return { type: "close_cursor" };
    }
    else if (type === "fetch_cursor") {
        const entries = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .arrayObjectsMap */ .Ki(obj["entries"], _shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .CursorEntry */ .pQ);
        const done = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .boolean */ .zM(obj["done"]);
        return { type: "fetch_cursor", entries, done };
    }
    else if (type === "sequence") {
        return { type: "sequence" };
    }
    else if (type === "describe") {
        const result = (0,_shared_json_decode_js__WEBPACK_IMPORTED_MODULE_2__/* .DescribeResult */ .J4)(_encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .object */ .Ik(obj["result"]));
        return { type: "describe", result };
    }
    else if (type === "store_sql") {
        return { type: "store_sql" };
    }
    else if (type === "close_sql") {
        return { type: "close_sql" };
    }
    else if (type === "get_autocommit") {
        const isAutocommit = _encoding_json_decode_js__WEBPACK_IMPORTED_MODULE_1__/* .boolean */ .zM(obj["is_autocommit"]);
        return { type: "get_autocommit", isAutocommit };
    }
    else {
        throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ProtoError */ .pO("Unexpected type of Response");
    }
}


/***/ },

/***/ 3349
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   l: () => (/* binding */ ClientMsg)
/* harmony export */ });
/* harmony import */ var _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(1958);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5233);


function ClientMsg(w, msg) {
    w.stringRaw("type", msg.type);
    if (msg.type === "hello") {
        if (msg.jwt !== undefined) {
            w.string("jwt", msg.jwt);
        }
    }
    else if (msg.type === "request") {
        w.number("request_id", msg.requestId);
        w.object("request", msg.request, Request);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of ClientMsg");
    }
}
function Request(w, msg) {
    w.stringRaw("type", msg.type);
    if (msg.type === "open_stream") {
        w.number("stream_id", msg.streamId);
    }
    else if (msg.type === "close_stream") {
        w.number("stream_id", msg.streamId);
    }
    else if (msg.type === "execute") {
        w.number("stream_id", msg.streamId);
        w.object("stmt", msg.stmt, _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Stmt */ .X);
    }
    else if (msg.type === "batch") {
        w.number("stream_id", msg.streamId);
        w.object("batch", msg.batch, _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
    }
    else if (msg.type === "open_cursor") {
        w.number("stream_id", msg.streamId);
        w.number("cursor_id", msg.cursorId);
        w.object("batch", msg.batch, _shared_json_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
    }
    else if (msg.type === "close_cursor") {
        w.number("cursor_id", msg.cursorId);
    }
    else if (msg.type === "fetch_cursor") {
        w.number("cursor_id", msg.cursorId);
        w.number("max_count", msg.maxCount);
    }
    else if (msg.type === "sequence") {
        w.number("stream_id", msg.streamId);
        if (msg.sql !== undefined) {
            w.string("sql", msg.sql);
        }
        if (msg.sqlId !== undefined) {
            w.number("sql_id", msg.sqlId);
        }
    }
    else if (msg.type === "describe") {
        w.number("stream_id", msg.streamId);
        if (msg.sql !== undefined) {
            w.string("sql", msg.sql);
        }
        if (msg.sqlId !== undefined) {
            w.number("sql_id", msg.sqlId);
        }
    }
    else if (msg.type === "store_sql") {
        w.number("sql_id", msg.sqlId);
        w.string("sql", msg.sql);
    }
    else if (msg.type === "close_sql") {
        w.number("sql_id", msg.sqlId);
    }
    else if (msg.type === "get_autocommit") {
        w.number("stream_id", msg.streamId);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of Request");
    }
}


/***/ },

/***/ 5474
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony import */ var _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(2845);

const ServerMsg = {
    default() { return { type: "none" }; },
    1(r) { return { type: "hello_ok" }; },
    2(r) { return r.message(HelloErrorMsg); },
    3(r) { return r.message(ResponseOkMsg); },
    4(r) { return r.message(ResponseErrorMsg); },
};
const HelloErrorMsg = {
    default() { return { type: "hello_error", error: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .Error */ .$D.default() }; },
    1(r, msg) { msg.error = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .Error */ .$D); },
};
const ResponseErrorMsg = {
    default() { return { type: "response_error", requestId: 0, error: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .Error */ .$D.default() }; },
    1(r, msg) { msg.requestId = r.int32(); },
    2(r, msg) { msg.error = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .Error */ .$D); },
};
const ResponseOkMsg = {
    default() {
        return {
            type: "response_ok",
            requestId: 0,
            response: { type: "none" },
        };
    },
    1(r, msg) { msg.requestId = r.int32(); },
    2(r, msg) { msg.response = { type: "open_stream" }; },
    3(r, msg) { msg.response = { type: "close_stream" }; },
    4(r, msg) { msg.response = r.message(ExecuteResp); },
    5(r, msg) { msg.response = r.message(BatchResp); },
    6(r, msg) { msg.response = { type: "open_cursor" }; },
    7(r, msg) { msg.response = { type: "close_cursor" }; },
    8(r, msg) { msg.response = r.message(FetchCursorResp); },
    9(r, msg) { msg.response = { type: "sequence" }; },
    10(r, msg) { msg.response = r.message(DescribeResp); },
    11(r, msg) { msg.response = { type: "store_sql" }; },
    12(r, msg) { msg.response = { type: "close_sql" }; },
    13(r, msg) { msg.response = r.message(GetAutocommitResp); },
};
const ExecuteResp = {
    default() { return { type: "execute", result: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .StmtResult */ .Ak.default() }; },
    1(r, msg) { msg.result = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .StmtResult */ .Ak); },
};
const BatchResp = {
    default() { return { type: "batch", result: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .BatchResult */ .et.default() }; },
    1(r, msg) { msg.result = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .BatchResult */ .et); },
};
const FetchCursorResp = {
    default() { return { type: "fetch_cursor", entries: [], done: false }; },
    1(r, msg) { msg.entries.push(r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .CursorEntry */ .pQ)); },
    2(r, msg) { msg.done = r.bool(); },
};
const DescribeResp = {
    default() { return { type: "describe", result: _shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .DescribeResult */ .J4.default() }; },
    1(r, msg) { msg.result = r.message(_shared_protobuf_decode_js__WEBPACK_IMPORTED_MODULE_0__/* .DescribeResult */ .J4); },
};
const GetAutocommitResp = {
    default() { return { type: "get_autocommit", isAutocommit: false }; },
    1(r, msg) { msg.isAutocommit = r.bool(); },
};

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "V", 0, /* binding */ ServerMsg
/* harmony export */ ]);


/***/ },

/***/ 6470
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   l: () => (/* binding */ ClientMsg)
/* harmony export */ });
/* harmony import */ var _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(6325);
/* harmony import */ var _util_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5233);


function ClientMsg(w, msg) {
    if (msg.type === "hello") {
        w.message(1, msg, HelloMsg);
    }
    else if (msg.type === "request") {
        w.message(2, msg, RequestMsg);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(msg, "Impossible type of ClientMsg");
    }
}
function HelloMsg(w, msg) {
    if (msg.jwt !== undefined) {
        w.string(1, msg.jwt);
    }
}
function RequestMsg(w, msg) {
    w.int32(1, msg.requestId);
    const request = msg.request;
    if (request.type === "open_stream") {
        w.message(2, request, OpenStreamReq);
    }
    else if (request.type === "close_stream") {
        w.message(3, request, CloseStreamReq);
    }
    else if (request.type === "execute") {
        w.message(4, request, ExecuteReq);
    }
    else if (request.type === "batch") {
        w.message(5, request, BatchReq);
    }
    else if (request.type === "open_cursor") {
        w.message(6, request, OpenCursorReq);
    }
    else if (request.type === "close_cursor") {
        w.message(7, request, CloseCursorReq);
    }
    else if (request.type === "fetch_cursor") {
        w.message(8, request, FetchCursorReq);
    }
    else if (request.type === "sequence") {
        w.message(9, request, SequenceReq);
    }
    else if (request.type === "describe") {
        w.message(10, request, DescribeReq);
    }
    else if (request.type === "store_sql") {
        w.message(11, request, StoreSqlReq);
    }
    else if (request.type === "close_sql") {
        w.message(12, request, CloseSqlReq);
    }
    else if (request.type === "get_autocommit") {
        w.message(13, request, GetAutocommitReq);
    }
    else {
        throw (0,_util_js__WEBPACK_IMPORTED_MODULE_1__/* .impossible */ .y)(request, "Impossible type of Request");
    }
}
function OpenStreamReq(w, msg) {
    w.int32(1, msg.streamId);
}
function CloseStreamReq(w, msg) {
    w.int32(1, msg.streamId);
}
function ExecuteReq(w, msg) {
    w.int32(1, msg.streamId);
    w.message(2, msg.stmt, _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Stmt */ .X);
}
function BatchReq(w, msg) {
    w.int32(1, msg.streamId);
    w.message(2, msg.batch, _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
}
function OpenCursorReq(w, msg) {
    w.int32(1, msg.streamId);
    w.int32(2, msg.cursorId);
    w.message(3, msg.batch, _shared_protobuf_encode_js__WEBPACK_IMPORTED_MODULE_0__/* .Batch */ .l);
}
function CloseCursorReq(w, msg) {
    w.int32(1, msg.cursorId);
}
function FetchCursorReq(w, msg) {
    w.int32(1, msg.cursorId);
    w.uint32(2, msg.maxCount);
}
function SequenceReq(w, msg) {
    w.int32(1, msg.streamId);
    if (msg.sql !== undefined) {
        w.string(2, msg.sql);
    }
    if (msg.sqlId !== undefined) {
        w.int32(3, msg.sqlId);
    }
}
function DescribeReq(w, msg) {
    w.int32(1, msg.streamId);
    if (msg.sql !== undefined) {
        w.string(2, msg.sql);
    }
    if (msg.sqlId !== undefined) {
        w.int32(3, msg.sqlId);
    }
}
function StoreSqlReq(w, msg) {
    w.int32(1, msg.sqlId);
    w.string(2, msg.sql);
}
function CloseSqlReq(w, msg) {
    w.int32(1, msg.sqlId);
}
function GetAutocommitReq(w, msg) {
    w.int32(1, msg.streamId);
}


/***/ },

/***/ 2570
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   P: () => (/* binding */ WsStream)
/* harmony export */ });
/* harmony import */ var _errors_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(8910);
/* harmony import */ var _queue_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(4870);
/* harmony import */ var _stream_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(5389);
/* harmony import */ var _cursor_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(4048);




class WsStream extends _stream_js__WEBPACK_IMPORTED_MODULE_2__/* .Stream */ .Z {
    #client;
    #streamId;
    #queue;
    #cursor;
    #closing;
    #closed;
    /** @private */
    static open(client) {
        const streamId = client._streamIdAlloc.alloc();
        const stream = new WsStream(client, streamId);
        const responseCallback = () => undefined;
        const errorCallback = (e) => stream.#setClosed(e);
        const request = { type: "open_stream", streamId };
        client._sendRequest(request, { responseCallback, errorCallback });
        return stream;
    }
    /** @private */
    constructor(client, streamId) {
        super(client.intMode);
        this.#client = client;
        this.#streamId = streamId;
        this.#queue = new _queue_js__WEBPACK_IMPORTED_MODULE_1__/* .Queue */ .o();
        this.#cursor = undefined;
        this.#closing = false;
        this.#closed = undefined;
    }
    /** Get the {@link WsClient} object that this stream belongs to. */
    client() {
        return this.#client;
    }
    /** @private */
    _sqlOwner() {
        return this.#client;
    }
    /** @private */
    _execute(stmt) {
        return this.#sendStreamRequest({
            type: "execute",
            streamId: this.#streamId,
            stmt,
        }).then((response) => {
            return response.result;
        });
    }
    /** @private */
    _batch(batch) {
        return this.#sendStreamRequest({
            type: "batch",
            streamId: this.#streamId,
            batch,
        }).then((response) => {
            return response.result;
        });
    }
    /** @private */
    _describe(protoSql) {
        this.#client._ensureVersion(2, "describe()");
        return this.#sendStreamRequest({
            type: "describe",
            streamId: this.#streamId,
            sql: protoSql.sql,
            sqlId: protoSql.sqlId,
        }).then((response) => {
            return response.result;
        });
    }
    /** @private */
    _sequence(protoSql) {
        this.#client._ensureVersion(2, "sequence()");
        return this.#sendStreamRequest({
            type: "sequence",
            streamId: this.#streamId,
            sql: protoSql.sql,
            sqlId: protoSql.sqlId,
        }).then((_response) => {
            return undefined;
        });
    }
    /** Check whether the SQL connection underlying this stream is in autocommit state (i.e., outside of an
     * explicit transaction). This requires protocol version 3 or higher.
     */
    getAutocommit() {
        this.#client._ensureVersion(3, "getAutocommit()");
        return this.#sendStreamRequest({
            type: "get_autocommit",
            streamId: this.#streamId,
        }).then((response) => {
            return response.isAutocommit;
        });
    }
    #sendStreamRequest(request) {
        return new Promise((responseCallback, errorCallback) => {
            this.#pushToQueue({ type: "request", request, responseCallback, errorCallback });
        });
    }
    /** @private */
    _openCursor(batch) {
        this.#client._ensureVersion(3, "cursor");
        return new Promise((cursorCallback, errorCallback) => {
            this.#pushToQueue({ type: "cursor", batch, cursorCallback, errorCallback });
        });
    }
    /** @private */
    _sendCursorRequest(cursor, request) {
        if (cursor !== this.#cursor) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .InternalError */ .Gd("Cursor not associated with the stream attempted to execute a request");
        }
        return new Promise((responseCallback, errorCallback) => {
            if (this.#closed !== undefined) {
                errorCallback(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClosedError */ .l8("Stream is closed", this.#closed));
            }
            else {
                this.#client._sendRequest(request, { responseCallback, errorCallback });
            }
        });
    }
    /** @private */
    _cursorClosed(cursor) {
        if (cursor !== this.#cursor) {
            throw new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .InternalError */ .Gd("Cursor was closed, but it was not associated with the stream");
        }
        this.#cursor = undefined;
        this.#flushQueue();
    }
    #pushToQueue(entry) {
        if (this.#closed !== undefined) {
            entry.errorCallback(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClosedError */ .l8("Stream is closed", this.#closed));
        }
        else if (this.#closing) {
            entry.errorCallback(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClosedError */ .l8("Stream is closing", undefined));
        }
        else {
            this.#queue.push(entry);
            this.#flushQueue();
        }
    }
    #flushQueue() {
        for (;;) {
            const entry = this.#queue.first();
            if (entry === undefined && this.#cursor === undefined && this.#closing) {
                this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClientError */ .MZ("Stream was gracefully closed"));
                break;
            }
            else if (entry?.type === "request" && this.#cursor === undefined) {
                const { request, responseCallback, errorCallback } = entry;
                this.#queue.shift();
                this.#client._sendRequest(request, { responseCallback, errorCallback });
            }
            else if (entry?.type === "cursor" && this.#cursor === undefined) {
                const { batch, cursorCallback } = entry;
                this.#queue.shift();
                const cursorId = this.#client._cursorIdAlloc.alloc();
                const cursor = new _cursor_js__WEBPACK_IMPORTED_MODULE_3__/* .WsCursor */ .V(this.#client, this, cursorId);
                const request = {
                    type: "open_cursor",
                    streamId: this.#streamId,
                    cursorId,
                    batch,
                };
                const responseCallback = () => undefined;
                const errorCallback = (e) => cursor._setClosed(e);
                this.#client._sendRequest(request, { responseCallback, errorCallback });
                this.#cursor = cursor;
                cursorCallback(cursor);
            }
            else {
                break;
            }
        }
    }
    #setClosed(error) {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;
        if (this.#cursor !== undefined) {
            this.#cursor._setClosed(error);
        }
        for (;;) {
            const entry = this.#queue.shift();
            if (entry !== undefined) {
                entry.errorCallback(error);
            }
            else {
                break;
            }
        }
        const request = { type: "close_stream", streamId: this.#streamId };
        const responseCallback = () => this.#client._streamIdAlloc.free(this.#streamId);
        const errorCallback = () => undefined;
        this.#client._sendRequest(request, { responseCallback, errorCallback });
    }
    /** Immediately close the stream. */
    close() {
        this.#setClosed(new _errors_js__WEBPACK_IMPORTED_MODULE_0__/* .ClientError */ .MZ("Stream was manually closed"));
    }
    /** Gracefully close the stream. */
    closeGracefully() {
        this.#closing = true;
        this.#flushQueue();
    }
    /** True if the stream is closed or closing. */
    get closed() {
        return this.#closed !== undefined || this.#closing;
    }
}


/***/ },

/***/ 451
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   hd: () => (/* binding */ fetchWithAgentSelection)
/* harmony export */ });
/* harmony import */ var http__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(3782);
/* harmony import */ var https__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(3871);



const _Request = Request;
const _Headers = Headers;

const httpAgent = new http__WEBPACK_IMPORTED_MODULE_0__["default"].Agent({ keepAlive: true });
const httpsAgent = new https__WEBPACK_IMPORTED_MODULE_1__["default"].Agent({ keepAlive: true });

function agentSelector(parsedUrl) {
    if (parsedUrl.protocol === 'https:') {
        return httpsAgent;
    } else {
        return httpAgent;
    }
}

function fetchWithAgentSelection(resource, options = {}) {
    return fetch(resource, { agent: agentSelector, ...options });
}

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "Kd", 0, /* binding */ _Request,
/* harmony export */   "Lr", 0, /* binding */ _Headers
/* harmony export */ ]);


/***/ },

/***/ 6346
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   k: () => (/* reexport safe */ ws__WEBPACK_IMPORTED_MODULE_0__.kb)
/* harmony export */ });
/* harmony import */ var ws__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(1259);




/***/ },

/***/ 5119
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   Nw: () => (/* binding */ searchData)
/* harmony export */ });
/* unused harmony exports isEqualArray, isEqualObject, queryData, sortData */
/* unused harmony import specifier */ var getValueFromObject;
/* unused harmony import specifier */ var isValidDate;


function isEqualArray(arr1, arr2) {
  if (!Array.isArray(arr1) || !Array.isArray(arr2)) {
    return false;
  }
  if (arr1.length !== arr2.length) {
    return false;
  }
  for (let i = 0; i < arr1.length; i++) {
    if (!isEqualValue(arr1[i], arr2[i])) {
      return false;
    }
  }
  return true;
}
function isEqualObject(obj1, obj2) {
  if (Object.is(obj1, obj2)) return true;
  if (!obj1 || !obj2 || typeof obj1 !== "object" || typeof obj2 !== "object") {
    return false;
  }
  if (obj1 instanceof Date || obj2 instanceof Date) {
    return obj1 instanceof Date && obj2 instanceof Date && obj1.getTime() === obj2.getTime();
  }
  if (Array.isArray(obj1) || Array.isArray(obj2)) {
    return isEqualArray(obj1, obj2);
  }
  const keys1 = Object.keys(obj1);
  const keys2 = Object.keys(obj2);
  if (keys1.length !== keys2.length) {
    return false;
  }
  for (const key of keys1) {
    if (!Object.prototype.hasOwnProperty.call(obj2, key) || !isEqualValue(obj1[key], obj2[key])) {
      return false;
    }
  }
  return true;
}
function isEqualValue(value1, value2) {
  if (Object.is(value1, value2)) return true;
  if (Array.isArray(value1) || Array.isArray(value2)) {
    return isEqualArray(value1, value2);
  }
  if (value1 && value2 && typeof value1 === "object" && typeof value2 === "object") {
    return isEqualObject(value1, value2);
  }
  return false;
}
function queryMatch(data, query) {
  for (let key of Object.keys(query)) {
    const queryEntry = query[key];
    let dataValue;
    try {
      dataValue = getValueFromObject(data, key, true);
    } catch (error) {
      return false;
    }
    if (queryEntry === null || typeof queryEntry !== "object") {
      if (Array.isArray(dataValue)) return dataValue.includes(queryEntry);
      else return Object.is(dataValue, queryEntry);
    } else if (Array.isArray(queryEntry)) {
      if (Array.isArray(dataValue)) {
        return isEqualArray(dataValue, queryEntry);
      } else {
        return false;
      }
    } else {
      for (let property of Object.keys(queryEntry)) {
        if (property === "$options") continue;
        if (!property.startsWith("$")) {
          if (typeof dataValue !== "object") {
            return false;
          } else
            return queryMatch(
              { [property]: getValueFromObject(dataValue, property) },
              { [property]: query[key][property] }
            );
        } else {
          let queryValue = queryEntry[property];
          if (isValidDate(queryValue) && isValidDate(dataValue)) {
            queryValue = new Date(queryValue);
            dataValue = new Date(dataValue);
          }
          let queryStatus = false;
          switch (property) {
            case "$eq":
              queryStatus = isEqualValue(dataValue, queryValue);
              break;
            case "$ne":
              queryStatus = !isEqualValue(dataValue, queryValue);
              break;
            case "$not":
              queryStatus = !queryMatch(data, {
                [key]: query[key]["$not"]
              });
              break;
            case "$lt":
              queryStatus = dataValue < queryValue;
              break;
            case "$lte":
              queryStatus = dataValue <= queryValue;
              break;
            case "$gt":
              queryStatus = dataValue > queryValue;
              break;
            case "$gte":
              queryStatus = dataValue >= queryValue;
              break;
            case "$in":
              queryValue = Array.isArray(queryValue) ? queryValue : [queryValue];
              if (Array.isArray(dataValue)) {
                queryStatus = dataValue.some(
                  (element) => queryValue.includes(element)
                );
              } else {
                queryStatus = queryValue.includes(dataValue);
              }
              break;
            case "$nin":
              queryValue = Array.isArray(queryValue) ? queryValue : [queryValue];
              if (Array.isArray(dataValue)) {
                queryStatus = !dataValue.some(
                  (element) => queryValue.includes(element)
                );
              } else {
                queryStatus = !queryValue.includes(dataValue);
              }
              break;
            case "$all":
              if (Array.isArray(dataValue) && Array.isArray(queryValue)) {
                queryStatus = queryValue.every(
                  (element) => dataValue.includes(element)
                );
              }
              break;
            case "$elemMatch":
              if (Array.isArray(data[key])) {
                queryStatus = data[key].some(
                  (element) => queryMatch(element, query[key][property])
                );
              }
              break;
            case "$size":
              if (Array.isArray(dataValue)) {
                queryStatus = dataValue.length === queryValue;
              }
              break;
            case "$exists":
              queryStatus = queryValue ? data.hasOwnProperty(key) : !data.hasOwnProperty(key);
              break;
            case "$regex":
              if (typeof dataValue === "string") {
                let regexFlag = query[key]["$options"] || "";
                let regex = new RegExp(queryValue, regexFlag);
                queryStatus = regex.test(dataValue);
              }
              break;
            case "$type":
              let dataType = typeof dataValue;
              if (Array.isArray(dataValue)) {
                dataType = "array";
              }
              queryStatus = dataType === queryValue;
              break;
            case "$mod":
              if (typeof dataValue === "number" && Array.isArray(queryValue) && queryValue.length === 2) {
                const [divisor, remainder] = queryValue;
                queryStatus = dataValue % divisor === remainder;
              }
              break;
            case "$where":
              if (typeof queryValue === "function") {
                try {
                } catch (error) {
                  console.error(
                    "Error in queryData $where function:",
                    error
                  );
                }
              }
              break;
            default:
              console.log("unknown operator");
              break;
          }
          if (!queryStatus) return false;
        }
      }
      return true;
    }
  }
}
function queryData(data, query) {
  if (!query) return true;
  if (query.$and) {
    for (let i = 0; i < query.$and.length; i++) {
      if (!queryData(data, query.$and[i])) return false;
    }
  }
  if (query.$nor) {
    for (let i = 0; i < query.$nor.length; i++) {
      if (queryData(data, query.$nor[i])) return false;
    }
  }
  for (let key of Object.keys(query)) {
    if (key === "$and" || key === "$or") continue;
    if (!queryMatch(data, { [key]: query[key] })) return false;
  }
  if (query.$or) {
    for (let i = 0; i < query.$or.length; i++) {
      if (queryData(data, query.$or[i])) return true;
    }
  }
  return true;
}
function searchData(data, search) {
  if (!search) return true;
  if (!Array.isArray(search)) search = [search];
  for (let i = 0; i < search.length; i++) {
    let searchValue = search[i].value;
    if (!Array.isArray(searchValue)) searchValue = [searchValue];
    const caseSensitive = search[i].caseSensitive === true || search[i].caseSensitive === "true";
    searchValue = searchValue.map((value) => {
      const text = String(value ?? "");
      return caseSensitive ? text : text.toLowerCase();
    });
    for (let key in data) {
      let value = data[key];
      let status = false;
      if (value === null || value === void 0) value = "";
      switch (typeof value) {
        case "number":
        case "boolean":
        case "bigint":
        case "symbol":
          value = value.toString();
          break;
        case "object":
          value = JSON.stringify(value) || "";
          break;
        case "function":
          value = value.toString();
          break;
      }
      if (!caseSensitive) value = value.toLowerCase();
      for (let j = 0; j < searchValue.length; j++) {
        const searchString = searchValue[j];
        if (searchString === "" && search[i].operator === "and") {
          if (value !== "") return false;
        }
        if (value.indexOf(searchString) > -1) status = true;
        if (status) return true;
        else if (search[i].operator == "and") return false;
      }
    }
    if (searchValue.some((value) => value.length > 0) && search[i].operator === "or") return false;
  }
  return true;
}
function getMongoTypeOrder(value) {
  if (value?._bsontype === "MinKey") return 0;
  if (value == null) return 1;
  if (typeof value === "number" || typeof value === "bigint") return 2;
  if (typeof value === "string" || typeof value === "symbol") return 3;
  if (Array.isArray(value)) return value.length ? 5 : 0.5;
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) return 6;
  if (value?._bsontype === "ObjectId" || value?.constructor?.name === "ObjectId")
    return 7;
  if (typeof value === "boolean") return 8;
  if (value instanceof Date) return 9;
  if (value?._bsontype === "Timestamp") return 10;
  if (value instanceof RegExp) return 11;
  if (typeof value === "function") return 12;
  if (value?._bsontype === "MaxKey") return 14;
  if (typeof value === "object") return 4;
  return 13;
}
function compareStrings(a, b) {
  const aString = String(a);
  const bString = String(b);
  if (aString === bString) return 0;
  return aString < bString ? -1 : 1;
}
function compareMongoObjects(a, b) {
  const aEntries = Object.entries(a);
  const bEntries = Object.entries(b);
  const length = Math.min(aEntries.length, bEntries.length);
  for (let index = 0; index < length; index++) {
    const [aKey, aValue] = aEntries[index];
    const [bKey, bValue] = bEntries[index];
    const typeComparison = getMongoTypeOrder(aValue) - getMongoTypeOrder(bValue);
    if (typeComparison) return Math.sign(typeComparison);
    const keyComparison = compareStrings(aKey, bKey);
    if (keyComparison) return keyComparison;
    const valueComparison = compareMongoValues(aValue, bValue);
    if (valueComparison) return valueComparison;
  }
  return Math.sign(aEntries.length - bEntries.length);
}
function getArraySortValue(value, direction) {
  if (!value.length) return value;
  const sorted = [...value].sort(compareMongoValues);
  return direction === "desc" ? sorted[sorted.length - 1] : sorted[0];
}
function compareMongoValues(a, b, direction = "asc") {
  if (Array.isArray(a)) a = getArraySortValue(a, direction);
  if (Array.isArray(b)) b = getArraySortValue(b, direction);
  if (Object.is(a, b) || a == null && b == null) return 0;
  const typeComparison = getMongoTypeOrder(a) - getMongoTypeOrder(b);
  if (typeComparison) return Math.sign(typeComparison);
  if (a == null || b == null) return a == null ? -1 : 1;
  if (typeof a === "number" || typeof a === "bigint")
    return a < b ? -1 : a > b ? 1 : 0;
  if (typeof a === "string" || typeof a === "symbol")
    return compareStrings(a, b);
  if (typeof a === "boolean") return Number(a) - Number(b);
  if (a instanceof Date) return Math.sign(a.getTime() - b.getTime());
  if (a instanceof RegExp) return compareStrings(a.toString(), b.toString());
  if (typeof a === "object") return compareMongoObjects(a, b);
  return compareStrings(a, b);
}
function sortData(data, sort) {
  return data.sort((a, b) => {
    for (let i = 0; i < sort.length; i++) {
      const key = sort[i].key;
      const direction = sort[i].direction === "desc" ? "desc" : "asc";
      const aValue = getValueFromObject(a, key);
      const bValue = getValueFromObject(b, key);
      const comparison = compareMongoValues(aValue, bValue, direction);
      if (comparison) return direction === "desc" ? -comparison : comparison;
    }
    return 0;
  });
}


/***/ },

/***/ 5604
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   O: () => (/* binding */ dotNotationToObject)
/* harmony export */ });
function dotNotationToObject(data = {}, obj = {}) {
  try {
    data ??= {};
    obj ??= {};
    let arrayGroup = {};
    for (const key of Object.keys(data)) {
      let value = data[key];
      let newObject = obj;
      let oldObject = new Object(obj);
      let keys = key.split(".");
      let length = keys.length - 1;
      for (let i = 0; i < keys.length; i++) {
        if (keys[i].endsWith("]")) {
          if (keys[i].endsWith("[]")) {
            let baseKey = keys[i].slice(0, -2);
            if (!Array.isArray(newObject[baseKey])) {
              newObject[baseKey] = [];
            }
            if (length == i) {
              if (Array.isArray(value)) {
                newObject[baseKey].push(...value);
              } else {
                newObject[baseKey].push(value);
              }
            }
          } else if (/\[([0-9]+)\]/g.test(keys[i])) {
            let [k, index] = keys[i].split("[");
            index = index.slice(0, -1);
            if (!Array.isArray(newObject[k])) {
              newObject[k] = [];
            }
            if (length == i) {
              if (value === void 0) {
                newObject[k].splice(index, 1);
              } else {
                newObject[k][index] = value;
              }
            } else {
              newObject[k][index] = oldObject[k][index] || {};
              newObject = newObject[k][index];
              oldObject = oldObject[k][index];
            }
          } else if (/\[\w\]/g.test(keys[i])) {
            let [k, group] = keys[i].split("[");
            group = group.slice(0, -1);
            if (!Array.isArray(newObject[k])) {
              newObject[k] = [];
            }
            let index;
            if (arrayGroup[keys.slice(0, i + 1).join(".")]) {
              index = arrayGroup[keys.slice(0, i + 1).join(".")];
            } else {
              index = newObject[k].length;
              arrayGroup[keys.slice(0, i + 1).join(".")] = index;
              newObject[k][index] = {};
            }
            if (length == i) {
              newObject[k][index] = value;
            } else {
              newObject = newObject[k][index];
            }
          }
        } else {
          if (length == i) {
            if (value === void 0) {
              delete newObject[keys[i]];
            } else {
              newObject[keys[i]] = value;
            }
          } else {
            newObject[keys[i]] = oldObject[keys[i]] || {};
            newObject = newObject[keys[i]];
            oldObject = oldObject[keys[i]];
          }
        }
      }
    }
    return obj;
  } catch (error) {
    console.log("Error converting dot notation to object", error);
    return false;
  }
}


/***/ },

/***/ 1224
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony import */ var _utilities_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(417);


function array(method, data) {
  const storageName = data.storageConfig?.name;
  return new Promise(
    async (resolve, reject) => {
      let type = "array";
      let arrayArray = [];
      try {
        const db = await (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .dbClient */ .KC)(data);
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
              if (data.filter && data.filter.query) {
                let isFilter = queryData(tableObj, data.filter.query);
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
              data = (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .createData */ .vd)(data, arrayArray, type);
              resolve(data);
            }
          } else {
            let arrays = data.array;
            if (method === "update") arrays = Object.entries(data.array);
            if (!Array.isArray(arrays)) arrays = [arrays];
            let arraysLength = arrays.length;
            for (let array2 of arrays) {
              if (method === "create") {
                const sql = `
									CREATE TABLE IF NOT EXISTS "${array2}" (
										_id TEXT PRIMARY KEY,
										organization_id TEXT,
										data TEXT DEFAULT '{}',
										created TEXT,
										updated TEXT
									);
								`;
                process.emit("usage", {
                  type: "egress",
                  data: { sql },
                  organization_id: data.organization_id
                });
                await db.execute(sql);
                arrayArray.push({
                  name: array2,
                  database,
                  storage: storageName
                });
                arraysLength -= 1;
                if (!arraysLength) databasesLength -= 1;
                if (!databasesLength && !arraysLength) {
                  data = (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .createData */ .vd)(data, arrayArray, type);
                  resolve(data);
                }
              } else {
                if (method === "update") {
                  let [oldName, newName] = array2;
                  const sql = `ALTER TABLE "${oldName}" RENAME TO "${newName}";`;
                  process.emit("usage", {
                    type: "egress",
                    data: { sql },
                    organization_id: data.organization_id
                  });
                  await db.execute(sql);
                  arrayArray.push({
                    name: newName,
                    oldName,
                    database,
                    storage: storageName
                  });
                  arraysLength -= 1;
                  if (!arraysLength) databasesLength -= 1;
                  if (!databasesLength && !arraysLength) {
                    data = (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .createData */ .vd)(data, arrayArray, type);
                    resolve(data);
                  }
                }
                if (method === "delete") {
                  const sql = `DROP TABLE IF EXISTS "${array2}";`;
                  process.emit("usage", {
                    type: "egress",
                    data: { sql },
                    organization_id: data.organization_id
                  });
                  await db.execute(sql);
                  arrayArray.push({
                    name: array2,
                    database,
                    storage: storageName
                  });
                  arraysLength -= 1;
                  if (!arraysLength) databasesLength -= 1;
                  if (!databasesLength && !arraysLength) {
                    data = (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .createData */ .vd)(data, arrayArray, type);
                    resolve(data);
                  }
                }
              }
            }
          }
        }
      } catch (error) {
        ;(0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .errorHandler */ .r_)(data, error);
        console.log(method, "error", error);
        resolve(data);
      }
    },
    (error) => {
      (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .errorHandler */ .r_)(data, error);
    }
  );
}
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (array);

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "A", 0, __WEBPACK_DEFAULT_EXPORT__
/* harmony export */ ]);


/***/ },

/***/ 9564
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony import */ var _utilities_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(417);


function database(method, data) {
  const storageName = data.storageConfig?.name;
  return new Promise(
    async (resolve, reject) => {
      let type = "database";
      let databaseArray = [];
      try {
        const db = await (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .dbClient */ .KC)(data);
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
          resolve((0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .createData */ .vd)(data, databaseArray, type));
        }
        if (method === "delete") {
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
        ;(0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .errorHandler */ .r_)(data, error);
        console.log(method, "error", error);
        resolve(data);
      }
    },
    (error) => {
      (0,_utilities_js__WEBPACK_IMPORTED_MODULE_0__/* .errorHandler */ .r_)(data, error);
    }
  );
}
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (database);

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "A", 0, __WEBPACK_DEFAULT_EXPORT__
/* harmony export */ ]);


/***/ },

/***/ 2210
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony import */ var _cocreate_utils__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(5604);
/* harmony import */ var _cocreate_utils__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5119);
/* harmony import */ var _utilities_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(417);


function object(method, data) {
  const storageName = data.storageConfig?.name;
  return new Promise(
    async (resolve, reject) => {
      try {
        const db = await (0,_utilities_js__WEBPACK_IMPORTED_MODULE_2__/* .dbClient */ .KC)(data);
        if (!db || db.status === false) return resolve(data);
        let type = "object";
        let documents = [];
        if (!data["timeStamp"]) {
          data["timeStamp"] = (/* @__PURE__ */ new Date()).toISOString();
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
            let isFilter = !!data.filter;
            if (isFilter && !data[type].length || data.isFilter) {
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
              let queryFilter = isFilter ? data.filter : data[type][i].filter || null;
              let { sqlWhere, params } = compileFilterToSQL(queryFilter, data.organization_id);
              if (method === "create") {
                data[type][i] = replaceArray(data[type][i]);
                data[type][i] = (0,_cocreate_utils__WEBPACK_IMPORTED_MODULE_0__/* .dotNotationToObject */ .O)(data[type][i]);
                delete data[type][i].modified;
                const id = data[type][i]._id || `row_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                const orgId = data.organization_id;
                const created = {
                  on: new Date(data.timeStamp),
                  by: data.user_id || data.clientId
                };
                const recordPayload = { ...data[type][i] };
                delete recordPayload._id;
                delete recordPayload.organization_id;
                delete recordPayload.created;
                delete recordPayload.updated;
                delete recordPayload.modified;
                const sql = `
									INSERT INTO "${array}" (_id, organization_id, data, created, updated)
									VALUES (?, ?, ?, ?, ?);
								`;
                const queryParams = [id, orgId, JSON.stringify(recordPayload), JSON.stringify(created), JSON.stringify(created)];
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
              } else if (method === "read") {
                let sql = `SELECT * FROM "${array}" ${sqlWhere}`;
                if (data.filter && data.filter.sort) {
                  const sortClauses = data.filter.sort.map((s) => {
                    let dir = s.direction === -1 || s.direction === "desc" ? "DESC" : "ASC";
                    if (["_id", "organization_id"].includes(s.key)) {
                      return `"${s.key}" ${dir}`;
                    }
                    return `json_extract(data, '$.${s.key}') ${dir}`;
                  });
                  sql += ` ORDER BY ${sortClauses.join(", ")}`;
                }
                if (data.filter && typeof data.filter.limit === "number") {
                  sql += ` LIMIT ${data.filter.limit}`;
                }
                if (data.filter && typeof data.filter.index === "number") {
                  sql += ` OFFSET ${data.filter.index}`;
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
                    if (data.filter && data.filter.search) {
                      let isMatch = (0,_cocreate_utils__WEBPACK_IMPORTED_MODULE_1__/* .searchData */ .Nw)(parsedRow, data.filter.search);
                      if (!isMatch) continue;
                    }
                    documents.push({
                      ...parsedRow,
                      ...reference
                    });
                  }
                } else if (result.rows.length > 0) {
                  let dbRecord = parseSqlRow(result.rows[0]);
                  if ($storage.length && data[type][i].updated && data[type][i].updated.on) {
                    let clientTime = new Date(data[type][i].updated.on);
                    let dbTime = new Date(dbRecord.updated ? dbRecord.updated.on : 0);
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
                      $storage,
                      $database,
                      $array
                    };
                  }
                } else {
                  data[type].splice(i, 1);
                  i--;
                }
              } else if (method === "update") {
                const updatePayload = { ...data[type][i] };
                delete updatePayload._id;
                delete updatePayload.filter;
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
              } else if (method === "delete") {
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
        data = (0,_utilities_js__WEBPACK_IMPORTED_MODULE_2__/* .createData */ .vd)(data, documents, type);
        resolve(data);
      } catch (error) {
        ;(0,_utilities_js__WEBPACK_IMPORTED_MODULE_2__/* .errorHandler */ .r_)(data, error);
        console.log(method, "error", error);
        resolve(data);
      }
    },
    (error) => {
      (0,_utilities_js__WEBPACK_IMPORTED_MODULE_2__/* .errorHandler */ .r_)(data, error);
    }
  );
}
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (object);

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "A", 0, __WEBPACK_DEFAULT_EXPORT__
/* harmony export */ ]);


/***/ },

/***/ 417
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   KC: () => (/* binding */ dbClient),
/* harmony export */   r_: () => (/* binding */ errorHandler),
/* harmony export */   vd: () => (/* binding */ createData)
/* harmony export */ });
/* unused harmony import specifier */ var queryData;
/* harmony import */ var _libsql_client__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(6282);


const organizations = {};
async function dbClient(data) {
  const storageUrl = data.storageConfig?.databaseUrl || data.storageConfig?.url;
  if (storageUrl) {
    if (!organizations[data.organization_id]) {
      organizations[data.organization_id] = {};
    }
    try {
      if (!organizations[data.organization_id][storageUrl]) {
        const connectionPromise = (async () => {
          let config = { url: storageUrl };
          if (data.storageConfig?.authToken || data.authToken || data.auth_token) {
            config.authToken = data.storageConfig?.authToken || data.authToken || data.auth_token;
          }
          const client = (0,_libsql_client__WEBPACK_IMPORTED_MODULE_0__/* .createClient */ .UU)(config);
          await client.execute("SELECT 1;");
          return client;
        })();
        organizations[data.organization_id][storageUrl] = connectionPromise;
      }
      return await organizations[data.organization_id][storageUrl];
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
    }, 5e3);
  }
});
async function executeUpdateTransaction(db, arrayName, docId, rawUpdateInput, requestData) {
  try {
    const selectSql = `SELECT * FROM "${arrayName}" WHERE _id = ? AND organization_id = ?;`;
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
    const updated = {
      on: new Date(requestData.timeStamp),
      by: requestData.user_id || requestData.clientId
    };
    const updatedPayload = applyMongoUpdate(currentRecord, rawUpdateInput);
    const savePayload = { ...updatedPayload };
    delete savePayload._id;
    delete savePayload.organization_id;
    delete savePayload.created;
    delete savePayload.updated;
    delete savePayload.modified;
    const updateSql = `
			UPDATE "${arrayName}"
			SET data = ?,
			    updated = ?
			WHERE _id = ? AND organization_id = ?;
		`;
    const updateParams = [JSON.stringify(savePayload), JSON.stringify(updated), docId, requestData.organization_id];
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
        arr = arr.filter((item) => {
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
        addValue.$each.forEach((item) => {
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
function compileFilterToSQL(filter, organizationId) {
  let clauses = ["organization_id = ?"];
  let params = [organizationId];
  if (filter && filter.query) {
    const parseConditionGroup = (queryObj, isOrJoin = false) => {
      let localClauses = [];
      if (queryObj.$or && Array.isArray(queryObj.$or)) {
        let orClauses = queryObj.$or.map((subQuery) => parseConditionGroup(subQuery)).filter((c) => c.length);
        if (orClauses.length) {
          localClauses.push("(" + orClauses.join(" OR ") + ")");
        }
      }
      if (queryObj.$and && Array.isArray(queryObj.$and)) {
        let andClauses = queryObj.$and.map((subQuery) => parseConditionGroup(subQuery)).filter((c) => c.length);
        if (andClauses.length) {
          localClauses.push("(" + andClauses.join(" AND ") + ")");
        }
      }
      for (let key in queryObj) {
        if (["$or", "$and"].includes(key)) continue;
        let value = queryObj[key];
        const targetField = ["_id", "organization_id"].includes(key) ? `"${key}"` : `json_extract(data, '$.${key}')`;
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
              params.pop();
              const placeholders = operand.map(() => "?").join(", ");
              operand.forEach((o) => params.push(o));
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
function parseSqlRow(row) {
  if (!row) return null;
  const recordData = typeof row.data === "string" ? JSON.parse(row.data) : row.data || {};
  return {
    _id: row._id,
    organization_id: row.organization_id,
    created: typeof row.created === "string" ? JSON.parse(row.created) : row.created,
    updated: typeof row.updated === "string" ? JSON.parse(row.updated) : row.updated,
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



/***/ },

/***/ 18
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* unused harmony exports VERSION, atob, atobPolyfill, btoa, btoaPolyfill, btou, decode, encode, encodeURI, encodeURL, extendBuiltins, extendString, extendUint8Array, fromBase64, fromUint8Array, isValid, toBase64, toUint8Array, utob, version */
/**
 *  base64.ts
 *
 *  Licensed under the BSD 3-Clause License.
 *    http://opensource.org/licenses/BSD-3-Clause
 *
 *  References:
 *    http://en.wikipedia.org/wiki/Base64
 *
 * @author Dan Kogai (https://github.com/dankogai)
 */
const version = '3.9.3';
/**
 * @deprecated use lowercase `version`.
 */
const VERSION = version;
const _TD = typeof TextDecoder === 'function' ? new TextDecoder('utf-8', { ignoreBOM: true }) : undefined;
const _TE = typeof TextEncoder === 'function' ? new TextEncoder() : undefined;
const b64ch = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
const b64chs = Array.prototype.slice.call(b64ch);
const b64tab = ((a) => {
    let tab = {};
    a.forEach((c, i) => tab[c] = i);
    return tab;
})(b64chs);
const b64re = /^(?:[A-Za-z\d+\/]{4})*?(?:[A-Za-z\d+\/]{2}(?:==)?|[A-Za-z\d+\/]{3}=?)?$/;
const _fromCC = String.fromCharCode.bind(String);
const _U8Afrom = typeof Uint8Array.from === 'function'
    ? Uint8Array.from.bind(Uint8Array)
    : (it) => new Uint8Array(Array.prototype.slice.call(it, 0));
const _mkUriSafe = (src) => src
    .replace(/=/g, '').replace(/[+\/]/g, (m0) => m0 == '+' ? '-' : '_');
const _tidyB64 = (s) => s.replace(/[^A-Za-z0-9\+\/]/g, '');
/**
 * polyfill version of `btoa`
 */
const btoaPolyfill = (bin) => {
    // console.log('polyfilled');
    let u32, c0, c1, c2, asc = '';
    const pad = bin.length % 3;
    for (let i = 0; i < bin.length;) {
        if ((c0 = bin.charCodeAt(i++)) > 255 ||
            (c1 = bin.charCodeAt(i++)) > 255 ||
            (c2 = bin.charCodeAt(i++)) > 255)
            throw new TypeError('invalid character found');
        u32 = (c0 << 16) | (c1 << 8) | c2;
        asc += b64chs[u32 >> 18 & 63]
            + b64chs[u32 >> 12 & 63]
            + b64chs[u32 >> 6 & 63]
            + b64chs[u32 & 63];
    }
    return pad ? asc.slice(0, pad - 3) + "===".substring(pad) : asc;
};
/**
 * does what `window.btoa` of web browsers do.
 * @param {String} bin binary string
 * @returns {string} Base64-encoded string
 */
const _btoa = typeof btoa === 'function' ? (bin) => btoa(bin)
    : btoaPolyfill;
const _fromUint8Array = typeof Uint8Array.prototype.toBase64 === 'function' ? (u8a) => u8a.toBase64()
    : (u8a) => {
        // cf. https://stackoverflow.com/questions/12710001/how-to-convert-uint8-array-to-base64-encoded-string/12713326#12713326
        const maxargs = 0x1000;
        let strs = [];
        for (let i = 0, l = u8a.length; i < l; i += maxargs) {
            strs.push(_fromCC.apply(null, u8a.subarray(i, i + maxargs)));
        }
        return _btoa(strs.join(''));
    };
/**
 * converts a Uint8Array to a Base64 string.
 * @param {boolean} [urlsafe] URL-and-filename-safe a la RFC4648 §5
 * @returns {string} Base64 string
 */
const fromUint8Array = (u8a, urlsafe = false) => urlsafe ? _mkUriSafe(_fromUint8Array(u8a)) : _fromUint8Array(u8a);
// This trick is found broken https://github.com/dankogai/js-base64/issues/130
// const utob = (src: string) => unescape(encodeURIComponent(src));
// reverting good old fationed regexp
const cb_utob = (c) => {
    if (c.length < 2) {
        var cc = c.charCodeAt(0);
        return cc < 0x80 ? c
            : cc < 0x800 ? (_fromCC(0xc0 | (cc >>> 6))
                + _fromCC(0x80 | (cc & 0x3f)))
                : (_fromCC(0xe0 | ((cc >>> 12) & 0x0f))
                    + _fromCC(0x80 | ((cc >>> 6) & 0x3f))
                    + _fromCC(0x80 | (cc & 0x3f)));
    }
    else {
        var cc = 0x10000
            + (c.charCodeAt(0) - 0xD800) * 0x400
            + (c.charCodeAt(1) - 0xDC00);
        return (_fromCC(0xf0 | ((cc >>> 18) & 0x07))
            + _fromCC(0x80 | ((cc >>> 12) & 0x3f))
            + _fromCC(0x80 | ((cc >>> 6) & 0x3f))
            + _fromCC(0x80 | (cc & 0x3f)));
    }
};
const re_utob = /[\uD800-\uDBFF][\uDC00-\uDFFF]|[^\x00-\x7F]/g;
/**
 * @deprecated should have been internal use only.
 * @param {string} src UTF-8 string
 * @returns {string} UTF-16 string
 */
const utob = (u) => u.replace(re_utob, cb_utob);
//
const _encode = _TE
    ? (s) => _fromUint8Array(_TE.encode(s))
    : (s) => _btoa(utob(s));
/**
 * converts a UTF-8-encoded string to a Base64 string.
 * @param {boolean} [urlsafe] if `true` make the result URL-safe
 * @returns {string} Base64 string
 */
const encode = (src, urlsafe = false) => urlsafe
    ? _mkUriSafe(_encode(src))
    : _encode(src);
/**
 * converts a UTF-8-encoded string to URL-safe Base64 RFC4648 §5.
 * @returns {string} Base64 string
 */
const encodeURI = (src) => encode(src, true);
// This trick is found broken https://github.com/dankogai/js-base64/issues/130
// const btou = (src: string) => decodeURIComponent(escape(src));
// reverting good old fationed regexp
const re_btou = /[\xC0-\xDF][\x80-\xBF]|[\xE0-\xEF][\x80-\xBF]{2}|[\xF0-\xF7][\x80-\xBF]{3}/g;
const cb_btou = (cccc) => {
    switch (cccc.length) {
        case 4:
            var cp = ((0x07 & cccc.charCodeAt(0)) << 18)
                | ((0x3f & cccc.charCodeAt(1)) << 12)
                | ((0x3f & cccc.charCodeAt(2)) << 6)
                | (0x3f & cccc.charCodeAt(3)), offset = cp - 0x10000;
            return (_fromCC((offset >>> 10) + 0xD800)
                + _fromCC((offset & 0x3FF) + 0xDC00));
        case 3:
            return _fromCC(((0x0f & cccc.charCodeAt(0)) << 12)
                | ((0x3f & cccc.charCodeAt(1)) << 6)
                | (0x3f & cccc.charCodeAt(2)));
        default:
            return _fromCC(((0x1f & cccc.charCodeAt(0)) << 6)
                | (0x3f & cccc.charCodeAt(1)));
    }
};
/**
 * @deprecated should have been internal use only.
 * @param {string} src UTF-16 string
 * @returns {string} UTF-8 string
 */
const btou = (b) => b.replace(re_btou, cb_btou);
/**
 * polyfill version of `atob`
 */
const atobPolyfill = (asc) => {
    // console.log('polyfilled');
    asc = asc.replace(/\s+/g, '');
    if (!b64re.test(asc))
        throw new TypeError('malformed base64.');
    asc += '=='.slice(2 - (asc.length & 3));
    let u24, r1, r2;
    let binArray = []; // use array to avoid minor gc in loop
    for (let i = 0; i < asc.length;) {
        u24 = b64tab[asc.charAt(i++)] << 18
            | b64tab[asc.charAt(i++)] << 12
            | (r1 = b64tab[asc.charAt(i++)]) << 6
            | (r2 = b64tab[asc.charAt(i++)]);
        if (r1 === 64) {
            binArray.push(_fromCC(u24 >> 16 & 255));
        }
        else if (r2 === 64) {
            binArray.push(_fromCC(u24 >> 16 & 255, u24 >> 8 & 255));
        }
        else {
            binArray.push(_fromCC(u24 >> 16 & 255, u24 >> 8 & 255, u24 & 255));
        }
    }
    return binArray.join('');
};
/**
 * does what `window.atob` of web browsers do.
 * @param {String} asc Base64-encoded string
 * @returns {string} binary string
 */
const _atob = typeof atob === 'function' ? (asc) => atob(_tidyB64(asc))
    : atobPolyfill;
//
const _toUint8Array = typeof Uint8Array.fromBase64 === 'function'
    ? (a) => Uint8Array.fromBase64(a) : (a) => _U8Afrom(_atob(a).split('').map(c => c.charCodeAt(0)));
/**
 * converts a Base64 string to a Uint8Array.
 */
const toUint8Array = (a) => _toUint8Array(_unURI(a));
//
const _decode = _TD
    ? (a) => _TD.decode(_toUint8Array(a))
    : (a) => btou(_atob(a));
const _unURI = (a) => _tidyB64(a.replace(/[-_]/g, (m0) => m0 == '-' ? '+' : '/'));
/**
 * converts a Base64 string to a UTF-8 string.
 * @param {String} src Base64 string.  Both normal and URL-safe are supported
 * @returns {string} UTF-8 string
 */
const decode = (src) => _decode(_unURI(src));
/**
 * check if a value is a valid Base64 string
 * @param {String} src a value to check
  */
const isValid = (src) => {
    if (typeof src !== 'string')
        return false;
    const s = src.replace(/\s+/g, '').replace(/={0,2}$/, '');
    return !/[^\s0-9a-zA-Z\+/]/.test(s) || !/[^\s0-9a-zA-Z\-_]/.test(s);
};
//
const _noEnum = (v) => {
    return {
        value: v, enumerable: false, writable: true, configurable: true
    };
};
/**
 * extend String.prototype with relevant methods
 */
const extendString = function () {
    const _add = (name, body) => Object.defineProperty(String.prototype, name, _noEnum(body));
    _add('fromBase64', function () { return decode(this); });
    _add('toBase64', function (urlsafe) { return encode(this, urlsafe); });
    _add('toBase64URI', function () { return encode(this, true); });
    _add('toBase64URL', function () { return encode(this, true); });
    _add('toUint8Array', function () { return toUint8Array(this); });
};
/**
 * extend Uint8Array.prototype with relevant methods
 */
const extendUint8Array = function () {
    const _add = (name, body) => Object.defineProperty(Uint8Array.prototype, name, _noEnum(body));
    _add('toBase64', function (urlsafe) { return fromUint8Array(this, urlsafe); });
    _add('toBase64URI', function () { return fromUint8Array(this, true); });
    _add('toBase64URL', function () { return fromUint8Array(this, true); });
};
/**
 * extend Builtin prototypes with relevant methods
 */
const extendBuiltins = () => {
    extendString();
    extendUint8Array();
};
const gBase64 = {
    version: version,
    VERSION: VERSION,
    atob: _atob,
    atobPolyfill: atobPolyfill,
    btoa: _btoa,
    btoaPolyfill: btoaPolyfill,
    fromBase64: decode,
    toBase64: encode,
    encode: encode,
    encodeURI: encodeURI,
    encodeURL: encodeURI,
    utob: utob,
    btou: btou,
    decode: decode,
    isValid: isValid,
    fromUint8Array: fromUint8Array,
    toUint8Array: toUint8Array,
    extendString: extendString,
    extendUint8Array: extendUint8Array,
    extendBuiltins: extendBuiltins
};



/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "o4", 0, /* binding */ gBase64
/* harmony export */ ]);


/***/ },

/***/ 3338
(module, __unused_webpack_exports, __webpack_require__) {



const { EMPTY_BUFFER } = __webpack_require__(2614);

const FastBuffer = Buffer[Symbol.species];

/**
 * Merges an array of buffers into a new buffer.
 *
 * @param {Buffer[]} list The array of buffers to concat
 * @param {Number} totalLength The total length of buffers in the list
 * @return {Buffer} The resulting buffer
 * @public
 */
function concat(list, totalLength) {
  if (list.length === 0) return EMPTY_BUFFER;
  if (list.length === 1) return list[0];

  const target = Buffer.allocUnsafe(totalLength);
  let offset = 0;

  for (let i = 0; i < list.length; i++) {
    const buf = list[i];
    target.set(buf, offset);
    offset += buf.length;
  }

  if (offset < totalLength) {
    return new FastBuffer(target.buffer, target.byteOffset, offset);
  }

  return target;
}

/**
 * Masks a buffer using the given mask.
 *
 * @param {Buffer} source The buffer to mask
 * @param {Buffer} mask The mask to use
 * @param {Buffer} output The buffer where to store the result
 * @param {Number} offset The offset at which to start writing
 * @param {Number} length The number of bytes to mask.
 * @public
 */
function _mask(source, mask, output, offset, length) {
  for (let i = 0; i < length; i++) {
    output[offset + i] = source[i] ^ mask[i & 3];
  }
}

/**
 * Unmasks a buffer using the given mask.
 *
 * @param {Buffer} buffer The buffer to unmask
 * @param {Buffer} mask The mask to use
 * @public
 */
function _unmask(buffer, mask) {
  for (let i = 0; i < buffer.length; i++) {
    buffer[i] ^= mask[i & 3];
  }
}

/**
 * Converts a buffer to an `ArrayBuffer`.
 *
 * @param {Buffer} buf The buffer to convert
 * @return {ArrayBuffer} Converted buffer
 * @public
 */
function toArrayBuffer(buf) {
  if (buf.length === buf.buffer.byteLength) {
    return buf.buffer;
  }

  return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
}

/**
 * Converts `data` to a `Buffer`.
 *
 * @param {*} data The data to convert
 * @return {Buffer} The buffer
 * @throws {TypeError}
 * @public
 */
function toBuffer(data) {
  toBuffer.readOnly = true;

  if (Buffer.isBuffer(data)) return data;

  let buf;

  if (data instanceof ArrayBuffer) {
    buf = new FastBuffer(data);
  } else if (ArrayBuffer.isView(data)) {
    buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
  } else {
    buf = Buffer.from(data);
    toBuffer.readOnly = false;
  }

  return buf;
}

module.exports = {
  concat,
  mask: _mask,
  toArrayBuffer,
  toBuffer,
  unmask: _unmask
};

/* istanbul ignore else  */
if (!process.env.WS_NO_BUFFER_UTIL) {
  try {
    const bufferUtil = __webpack_require__(Object(function webpackMissingModule() { const e = new Error("Cannot find module 'bufferutil'"); e.code = 'MODULE_NOT_FOUND'; throw e; }()));

    module.exports.mask = function (source, mask, output, offset, length) {
      if (length < 48) _mask(source, mask, output, offset, length);
      else bufferUtil.mask(source, mask, output, offset, length);
    };

    module.exports.unmask = function (buffer, mask) {
      if (buffer.length < 32) _unmask(buffer, mask);
      else bufferUtil.unmask(buffer, mask);
    };
  } catch (e) {
    // Continue regardless of the error.
  }
}


/***/ },

/***/ 2614
(module) {



const BINARY_TYPES = ['nodebuffer', 'arraybuffer', 'fragments'];
const hasBlob = typeof Blob !== 'undefined';

if (hasBlob) BINARY_TYPES.push('blob');

module.exports = {
  BINARY_TYPES,
  CLOSE_TIMEOUT: 30000,
  EMPTY_BUFFER: Buffer.alloc(0),
  GUID: '258EAFA5-E914-47DA-95CA-C5AB0DC85B11',
  hasBlob,
  kForOnEventAttribute: Symbol('kIsForOnEventAttribute'),
  kListener: Symbol('kListener'),
  kStatusCode: Symbol('status-code'),
  kWebSocket: Symbol('websocket'),
  NOOP: () => {}
};


/***/ },

/***/ 597
(module, __unused_webpack_exports, __webpack_require__) {



const { kForOnEventAttribute, kListener } = __webpack_require__(2614);

const kCode = Symbol('kCode');
const kData = Symbol('kData');
const kError = Symbol('kError');
const kMessage = Symbol('kMessage');
const kReason = Symbol('kReason');
const kTarget = Symbol('kTarget');
const kType = Symbol('kType');
const kWasClean = Symbol('kWasClean');

/**
 * Class representing an event.
 */
class Event {
  /**
   * Create a new `Event`.
   *
   * @param {String} type The name of the event
   * @throws {TypeError} If the `type` argument is not specified
   */
  constructor(type) {
    this[kTarget] = null;
    this[kType] = type;
  }

  /**
   * @type {*}
   */
  get target() {
    return this[kTarget];
  }

  /**
   * @type {String}
   */
  get type() {
    return this[kType];
  }
}

Object.defineProperty(Event.prototype, 'target', { enumerable: true });
Object.defineProperty(Event.prototype, 'type', { enumerable: true });

/**
 * Class representing a close event.
 *
 * @extends Event
 */
class CloseEvent extends Event {
  /**
   * Create a new `CloseEvent`.
   *
   * @param {String} type The name of the event
   * @param {Object} [options] A dictionary object that allows for setting
   *     attributes via object members of the same name
   * @param {Number} [options.code=0] The status code explaining why the
   *     connection was closed
   * @param {String} [options.reason=''] A human-readable string explaining why
   *     the connection was closed
   * @param {Boolean} [options.wasClean=false] Indicates whether or not the
   *     connection was cleanly closed
   */
  constructor(type, options = {}) {
    super(type);

    this[kCode] = options.code === undefined ? 0 : options.code;
    this[kReason] = options.reason === undefined ? '' : options.reason;
    this[kWasClean] = options.wasClean === undefined ? false : options.wasClean;
  }

  /**
   * @type {Number}
   */
  get code() {
    return this[kCode];
  }

  /**
   * @type {String}
   */
  get reason() {
    return this[kReason];
  }

  /**
   * @type {Boolean}
   */
  get wasClean() {
    return this[kWasClean];
  }
}

Object.defineProperty(CloseEvent.prototype, 'code', { enumerable: true });
Object.defineProperty(CloseEvent.prototype, 'reason', { enumerable: true });
Object.defineProperty(CloseEvent.prototype, 'wasClean', { enumerable: true });

/**
 * Class representing an error event.
 *
 * @extends Event
 */
class ErrorEvent extends Event {
  /**
   * Create a new `ErrorEvent`.
   *
   * @param {String} type The name of the event
   * @param {Object} [options] A dictionary object that allows for setting
   *     attributes via object members of the same name
   * @param {*} [options.error=null] The error that generated this event
   * @param {String} [options.message=''] The error message
   */
  constructor(type, options = {}) {
    super(type);

    this[kError] = options.error === undefined ? null : options.error;
    this[kMessage] = options.message === undefined ? '' : options.message;
  }

  /**
   * @type {*}
   */
  get error() {
    return this[kError];
  }

  /**
   * @type {String}
   */
  get message() {
    return this[kMessage];
  }
}

Object.defineProperty(ErrorEvent.prototype, 'error', { enumerable: true });
Object.defineProperty(ErrorEvent.prototype, 'message', { enumerable: true });

/**
 * Class representing a message event.
 *
 * @extends Event
 */
class MessageEvent extends Event {
  /**
   * Create a new `MessageEvent`.
   *
   * @param {String} type The name of the event
   * @param {Object} [options] A dictionary object that allows for setting
   *     attributes via object members of the same name
   * @param {*} [options.data=null] The message content
   */
  constructor(type, options = {}) {
    super(type);

    this[kData] = options.data === undefined ? null : options.data;
  }

  /**
   * @type {*}
   */
  get data() {
    return this[kData];
  }
}

Object.defineProperty(MessageEvent.prototype, 'data', { enumerable: true });

/**
 * This provides methods for emulating the `EventTarget` interface. It's not
 * meant to be used directly.
 *
 * @mixin
 */
const EventTarget = {
  /**
   * Register an event listener.
   *
   * @param {String} type A string representing the event type to listen for
   * @param {(Function|Object)} handler The listener to add
   * @param {Object} [options] An options object specifies characteristics about
   *     the event listener
   * @param {Boolean} [options.once=false] A `Boolean` indicating that the
   *     listener should be invoked at most once after being added. If `true`,
   *     the listener would be automatically removed when invoked.
   * @public
   */
  addEventListener(type, handler, options = {}) {
    for (const listener of this.listeners(type)) {
      if (
        !options[kForOnEventAttribute] &&
        listener[kListener] === handler &&
        !listener[kForOnEventAttribute]
      ) {
        return;
      }
    }

    let wrapper;

    if (type === 'message') {
      wrapper = function onMessage(data, isBinary) {
        const event = new MessageEvent('message', {
          data: isBinary ? data : data.toString()
        });

        event[kTarget] = this;
        callListener(handler, this, event);
      };
    } else if (type === 'close') {
      wrapper = function onClose(code, message) {
        const event = new CloseEvent('close', {
          code,
          reason: message.toString(),
          wasClean: this._closeFrameReceived && this._closeFrameSent
        });

        event[kTarget] = this;
        callListener(handler, this, event);
      };
    } else if (type === 'error') {
      wrapper = function onError(error) {
        const event = new ErrorEvent('error', {
          error,
          message: error.message
        });

        event[kTarget] = this;
        callListener(handler, this, event);
      };
    } else if (type === 'open') {
      wrapper = function onOpen() {
        const event = new Event('open');

        event[kTarget] = this;
        callListener(handler, this, event);
      };
    } else {
      return;
    }

    wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
    wrapper[kListener] = handler;

    if (options.once) {
      this.once(type, wrapper);
    } else {
      this.on(type, wrapper);
    }
  },

  /**
   * Remove an event listener.
   *
   * @param {String} type A string representing the event type to remove
   * @param {(Function|Object)} handler The listener to remove
   * @public
   */
  removeEventListener(type, handler) {
    for (const listener of this.listeners(type)) {
      if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
        this.removeListener(type, listener);
        break;
      }
    }
  }
};

module.exports = {
  ...void (CloseEvent),
  ...void (ErrorEvent),
  ...void (Event),
  EventTarget,
  ...void (MessageEvent)
};

/**
 * Call an event listener
 *
 * @param {(Function|Object)} listener The listener to call
 * @param {*} thisArg The value to use as `this`` when calling the listener
 * @param {Event} event The event to pass to the listener
 * @private
 */
function callListener(listener, thisArg, event) {
  if (typeof listener === 'object' && listener.handleEvent) {
    listener.handleEvent.call(listener, event);
  } else {
    listener.call(thisArg, event);
  }
}


/***/ },

/***/ 5926
(module, __unused_webpack_exports, __webpack_require__) {



const { tokenChars } = __webpack_require__(5880);

/**
 * Adds an offer to the map of extension offers or a parameter to the map of
 * parameters.
 *
 * @param {Object} dest The map of extension offers or parameters
 * @param {String} name The extension or parameter name
 * @param {(Object|Boolean|String)} elem The extension parameters or the
 *     parameter value
 * @private
 */
function push(dest, name, elem) {
  if (dest[name] === undefined) dest[name] = [elem];
  else dest[name].push(elem);
}

/**
 * Parses the `Sec-WebSocket-Extensions` header into an object.
 *
 * @param {String} header The field value of the header
 * @return {Object} The parsed object
 * @public
 */
function parse(header) {
  const offers = Object.create(null);
  let params = Object.create(null);
  let mustUnescape = false;
  let isEscaping = false;
  let inQuotes = false;
  let extensionName;
  let paramName;
  let start = -1;
  let code = -1;
  let end = -1;
  let i = 0;

  for (; i < header.length; i++) {
    code = header.charCodeAt(i);

    if (extensionName === undefined) {
      if (end === -1 && tokenChars[code] === 1) {
        if (start === -1) start = i;
      } else if (
        i !== 0 &&
        (code === 0x20 /* ' ' */ || code === 0x09) /* '\t' */
      ) {
        if (end === -1 && start !== -1) end = i;
      } else if (code === 0x3b /* ';' */ || code === 0x2c /* ',' */) {
        if (start === -1) {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }

        if (end === -1) end = i;
        const name = header.slice(start, end);
        if (code === 0x2c) {
          push(offers, name, params);
          params = Object.create(null);
        } else {
          extensionName = name;
        }

        start = end = -1;
      } else {
        throw new SyntaxError(`Unexpected character at index ${i}`);
      }
    } else if (paramName === undefined) {
      if (end === -1 && tokenChars[code] === 1) {
        if (start === -1) start = i;
      } else if (code === 0x20 || code === 0x09) {
        if (end === -1 && start !== -1) end = i;
      } else if (code === 0x3b || code === 0x2c) {
        if (start === -1) {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }

        if (end === -1) end = i;
        push(params, header.slice(start, end), true);
        if (code === 0x2c) {
          push(offers, extensionName, params);
          params = Object.create(null);
          extensionName = undefined;
        }

        start = end = -1;
      } else if (code === 0x3d /* '=' */ && start !== -1 && end === -1) {
        paramName = header.slice(start, i);
        start = end = -1;
      } else {
        throw new SyntaxError(`Unexpected character at index ${i}`);
      }
    } else {
      //
      // The value of a quoted-string after unescaping must conform to the
      // token ABNF, so only token characters are valid.
      // Ref: https://tools.ietf.org/html/rfc6455#section-9.1
      //
      if (isEscaping) {
        if (tokenChars[code] !== 1) {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
        if (start === -1) start = i;
        else if (!mustUnescape) mustUnescape = true;
        isEscaping = false;
      } else if (inQuotes) {
        if (tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (code === 0x22 /* '"' */ && start !== -1) {
          inQuotes = false;
          end = i;
        } else if (code === 0x5c /* '\' */) {
          isEscaping = true;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      } else if (code === 0x22 && header.charCodeAt(i - 1) === 0x3d) {
        inQuotes = true;
      } else if (end === -1 && tokenChars[code] === 1) {
        if (start === -1) start = i;
      } else if (start !== -1 && (code === 0x20 || code === 0x09)) {
        if (end === -1) end = i;
      } else if (code === 0x3b || code === 0x2c) {
        if (start === -1) {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }

        if (end === -1) end = i;
        let value = header.slice(start, end);
        if (mustUnescape) {
          value = value.replace(/\\/g, '');
          mustUnescape = false;
        }
        push(params, paramName, value);
        if (code === 0x2c) {
          push(offers, extensionName, params);
          params = Object.create(null);
          extensionName = undefined;
        }

        paramName = undefined;
        start = end = -1;
      } else {
        throw new SyntaxError(`Unexpected character at index ${i}`);
      }
    }
  }

  if (start === -1 || inQuotes || code === 0x20 || code === 0x09) {
    throw new SyntaxError('Unexpected end of input');
  }

  if (end === -1) end = i;
  const token = header.slice(start, end);
  if (extensionName === undefined) {
    push(offers, token, params);
  } else {
    if (paramName === undefined) {
      push(params, token, true);
    } else if (mustUnescape) {
      push(params, paramName, token.replace(/\\/g, ''));
    } else {
      push(params, paramName, token);
    }
    push(offers, extensionName, params);
  }

  return offers;
}

/**
 * Builds the `Sec-WebSocket-Extensions` header field value.
 *
 * @param {Object} extensions The map of extensions and parameters to format
 * @return {String} A string representing the given object
 * @public
 */
function format(extensions) {
  return Object.keys(extensions)
    .map((extension) => {
      let configurations = extensions[extension];
      if (!Array.isArray(configurations)) configurations = [configurations];
      return configurations
        .map((params) => {
          return [extension]
            .concat(
              Object.keys(params).map((k) => {
                let values = params[k];
                if (!Array.isArray(values)) values = [values];
                return values
                  .map((v) => (v === true ? k : `${k}=${v}`))
                  .join('; ');
              })
            )
            .join('; ');
        })
        .join(', ');
    })
    .join(', ');
}

module.exports = { format, parse };


/***/ },

/***/ 4759
(module) {



const kDone = Symbol('kDone');
const kRun = Symbol('kRun');

/**
 * A very simple job queue with adjustable concurrency. Adapted from
 * https://github.com/STRML/async-limiter
 */
class Limiter {
  /**
   * Creates a new `Limiter`.
   *
   * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
   *     to run concurrently
   */
  constructor(concurrency) {
    this[kDone] = () => {
      this.pending--;
      this[kRun]();
    };
    this.concurrency = concurrency || Infinity;
    this.jobs = [];
    this.pending = 0;
  }

  /**
   * Adds a job to the queue.
   *
   * @param {Function} job The job to run
   * @public
   */
  add(job) {
    this.jobs.push(job);
    this[kRun]();
  }

  /**
   * Removes a job from the queue and runs it if possible.
   *
   * @private
   */
  [kRun]() {
    if (this.pending === this.concurrency) return;

    if (this.jobs.length) {
      const job = this.jobs.shift();

      this.pending++;
      job(this[kDone]);
    }
  }
}

module.exports = Limiter;


/***/ },

/***/ 2971
(module, __unused_webpack_exports, __webpack_require__) {



const zlib = __webpack_require__(3106);

const bufferUtil = __webpack_require__(3338);
const Limiter = __webpack_require__(4759);
const { kStatusCode } = __webpack_require__(2614);

const FastBuffer = Buffer[Symbol.species];
const TRAILER = Buffer.from([0x00, 0x00, 0xff, 0xff]);
const kPerMessageDeflate = Symbol('permessage-deflate');
const kTotalLength = Symbol('total-length');
const kCallback = Symbol('callback');
const kBuffers = Symbol('buffers');
const kError = Symbol('error');

//
// We limit zlib concurrency, which prevents severe memory fragmentation
// as documented in https://github.com/nodejs/node/issues/8871#issuecomment-250915913
// and https://github.com/websockets/ws/issues/1202
//
// Intentionally global; it's the global thread pool that's an issue.
//
let zlibLimiter;

/**
 * permessage-deflate implementation.
 */
class PerMessageDeflate {
  /**
   * Creates a PerMessageDeflate instance.
   *
   * @param {Object} [options] Configuration options
   * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
   *     for, or request, a custom client window size
   * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
   *     acknowledge disabling of client context takeover
   * @param {Number} [options.concurrencyLimit=10] The number of concurrent
   *     calls to zlib
   * @param {Boolean} [options.isServer=false] Create the instance in either
   *     server or client mode
   * @param {Number} [options.maxPayload=0] The maximum allowed message length
   * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
   *     use of a custom server window size
   * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
   *     disabling of server context takeover
   * @param {Number} [options.threshold=1024] Size (in bytes) below which
   *     messages should not be compressed if context takeover is disabled
   * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
   *     deflate
   * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
   *     inflate
   */
  constructor(options) {
    this._options = options || {};
    this._threshold =
      this._options.threshold !== undefined ? this._options.threshold : 1024;
    this._maxPayload = this._options.maxPayload | 0;
    this._isServer = !!this._options.isServer;
    this._deflate = null;
    this._inflate = null;

    this.params = null;

    if (!zlibLimiter) {
      const concurrency =
        this._options.concurrencyLimit !== undefined
          ? this._options.concurrencyLimit
          : 10;
      zlibLimiter = new Limiter(concurrency);
    }
  }

  /**
   * @type {String}
   */
  static get extensionName() {
    return 'permessage-deflate';
  }

  /**
   * Create an extension negotiation offer.
   *
   * @return {Object} Extension parameters
   * @public
   */
  offer() {
    const params = {};

    if (this._options.serverNoContextTakeover) {
      params.server_no_context_takeover = true;
    }
    if (this._options.clientNoContextTakeover) {
      params.client_no_context_takeover = true;
    }
    if (this._options.serverMaxWindowBits) {
      params.server_max_window_bits = this._options.serverMaxWindowBits;
    }
    if (this._options.clientMaxWindowBits) {
      params.client_max_window_bits = this._options.clientMaxWindowBits;
    } else if (this._options.clientMaxWindowBits == null) {
      params.client_max_window_bits = true;
    }

    return params;
  }

  /**
   * Accept an extension negotiation offer/response.
   *
   * @param {Array} configurations The extension negotiation offers/reponse
   * @return {Object} Accepted configuration
   * @public
   */
  accept(configurations) {
    configurations = this.normalizeParams(configurations);

    this.params = this._isServer
      ? this.acceptAsServer(configurations)
      : this.acceptAsClient(configurations);

    return this.params;
  }

  /**
   * Releases all resources used by the extension.
   *
   * @public
   */
  cleanup() {
    if (this._inflate) {
      this._inflate.close();
      this._inflate = null;
    }

    if (this._deflate) {
      const callback = this._deflate[kCallback];

      this._deflate.close();
      this._deflate = null;

      if (callback) {
        callback(
          new Error(
            'The deflate stream was closed while data was being processed'
          )
        );
      }
    }
  }

  /**
   *  Accept an extension negotiation offer.
   *
   * @param {Array} offers The extension negotiation offers
   * @return {Object} Accepted configuration
   * @private
   */
  acceptAsServer(offers) {
    const opts = this._options;
    const accepted = offers.find((params) => {
      if (
        (opts.serverNoContextTakeover === false &&
          params.server_no_context_takeover) ||
        (params.server_max_window_bits &&
          (opts.serverMaxWindowBits === false ||
            (typeof opts.serverMaxWindowBits === 'number' &&
              opts.serverMaxWindowBits > params.server_max_window_bits))) ||
        (typeof opts.clientMaxWindowBits === 'number' &&
          (typeof params.client_max_window_bits === 'number'
            ? opts.clientMaxWindowBits > params.client_max_window_bits
            : !params.client_max_window_bits))
      ) {
        return false;
      }

      return true;
    });

    if (!accepted) {
      throw new Error('None of the extension offers can be accepted');
    }

    if (opts.serverNoContextTakeover) {
      accepted.server_no_context_takeover = true;
    }
    if (opts.clientNoContextTakeover) {
      accepted.client_no_context_takeover = true;
    }
    if (typeof opts.serverMaxWindowBits === 'number') {
      accepted.server_max_window_bits = opts.serverMaxWindowBits;
    }
    if (typeof opts.clientMaxWindowBits === 'number') {
      accepted.client_max_window_bits = opts.clientMaxWindowBits;
    } else if (
      accepted.client_max_window_bits === true ||
      opts.clientMaxWindowBits === false
    ) {
      delete accepted.client_max_window_bits;
    }

    return accepted;
  }

  /**
   * Accept the extension negotiation response.
   *
   * @param {Array} response The extension negotiation response
   * @return {Object} Accepted configuration
   * @private
   */
  acceptAsClient(response) {
    const params = response[0];

    if (
      this._options.clientNoContextTakeover === false &&
      params.client_no_context_takeover
    ) {
      throw new Error('Unexpected parameter "client_no_context_takeover"');
    }

    if (!params.client_max_window_bits) {
      if (typeof this._options.clientMaxWindowBits === 'number') {
        params.client_max_window_bits = this._options.clientMaxWindowBits;
      }
    } else if (
      this._options.clientMaxWindowBits === false ||
      (typeof this._options.clientMaxWindowBits === 'number' &&
        params.client_max_window_bits > this._options.clientMaxWindowBits)
    ) {
      throw new Error(
        'Unexpected or invalid parameter "client_max_window_bits"'
      );
    }

    return params;
  }

  /**
   * Normalize parameters.
   *
   * @param {Array} configurations The extension negotiation offers/reponse
   * @return {Array} The offers/response with normalized parameters
   * @private
   */
  normalizeParams(configurations) {
    configurations.forEach((params) => {
      Object.keys(params).forEach((key) => {
        let value = params[key];

        if (value.length > 1) {
          throw new Error(`Parameter "${key}" must have only a single value`);
        }

        value = value[0];

        if (key === 'client_max_window_bits') {
          if (value !== true) {
            const num = +value;
            if (!Number.isInteger(num) || num < 8 || num > 15) {
              throw new TypeError(
                `Invalid value for parameter "${key}": ${value}`
              );
            }
            value = num;
          } else if (!this._isServer) {
            throw new TypeError(
              `Invalid value for parameter "${key}": ${value}`
            );
          }
        } else if (key === 'server_max_window_bits') {
          const num = +value;
          if (!Number.isInteger(num) || num < 8 || num > 15) {
            throw new TypeError(
              `Invalid value for parameter "${key}": ${value}`
            );
          }
          value = num;
        } else if (
          key === 'client_no_context_takeover' ||
          key === 'server_no_context_takeover'
        ) {
          if (value !== true) {
            throw new TypeError(
              `Invalid value for parameter "${key}": ${value}`
            );
          }
        } else {
          throw new Error(`Unknown parameter "${key}"`);
        }

        params[key] = value;
      });
    });

    return configurations;
  }

  /**
   * Decompress data. Concurrency limited.
   *
   * @param {Buffer} data Compressed data
   * @param {Boolean} fin Specifies whether or not this is the last fragment
   * @param {Function} callback Callback
   * @public
   */
  decompress(data, fin, callback) {
    zlibLimiter.add((done) => {
      this._decompress(data, fin, (err, result) => {
        done();
        callback(err, result);
      });
    });
  }

  /**
   * Compress data. Concurrency limited.
   *
   * @param {(Buffer|String)} data Data to compress
   * @param {Boolean} fin Specifies whether or not this is the last fragment
   * @param {Function} callback Callback
   * @public
   */
  compress(data, fin, callback) {
    zlibLimiter.add((done) => {
      this._compress(data, fin, (err, result) => {
        done();
        callback(err, result);
      });
    });
  }

  /**
   * Decompress data.
   *
   * @param {Buffer} data Compressed data
   * @param {Boolean} fin Specifies whether or not this is the last fragment
   * @param {Function} callback Callback
   * @private
   */
  _decompress(data, fin, callback) {
    const endpoint = this._isServer ? 'client' : 'server';

    if (!this._inflate) {
      const key = `${endpoint}_max_window_bits`;
      const windowBits =
        typeof this.params[key] !== 'number'
          ? zlib.Z_DEFAULT_WINDOWBITS
          : this.params[key];

      this._inflate = zlib.createInflateRaw({
        ...this._options.zlibInflateOptions,
        windowBits
      });
      this._inflate[kPerMessageDeflate] = this;
      this._inflate[kTotalLength] = 0;
      this._inflate[kBuffers] = [];
      this._inflate.on('error', inflateOnError);
      this._inflate.on('data', inflateOnData);
    }

    this._inflate[kCallback] = callback;

    this._inflate.write(data);
    if (fin) this._inflate.write(TRAILER);

    this._inflate.flush(() => {
      const err = this._inflate[kError];

      if (err) {
        this._inflate.close();
        this._inflate = null;
        callback(err);
        return;
      }

      const data = bufferUtil.concat(
        this._inflate[kBuffers],
        this._inflate[kTotalLength]
      );

      if (this._inflate._readableState.endEmitted) {
        this._inflate.close();
        this._inflate = null;
      } else {
        this._inflate[kTotalLength] = 0;
        this._inflate[kBuffers] = [];

        if (fin && this.params[`${endpoint}_no_context_takeover`]) {
          this._inflate.reset();
        }
      }

      callback(null, data);
    });
  }

  /**
   * Compress data.
   *
   * @param {(Buffer|String)} data Data to compress
   * @param {Boolean} fin Specifies whether or not this is the last fragment
   * @param {Function} callback Callback
   * @private
   */
  _compress(data, fin, callback) {
    const endpoint = this._isServer ? 'server' : 'client';

    if (!this._deflate) {
      const key = `${endpoint}_max_window_bits`;
      const windowBits =
        typeof this.params[key] !== 'number'
          ? zlib.Z_DEFAULT_WINDOWBITS
          : this.params[key];

      this._deflate = zlib.createDeflateRaw({
        ...this._options.zlibDeflateOptions,
        windowBits
      });

      this._deflate[kTotalLength] = 0;
      this._deflate[kBuffers] = [];

      this._deflate.on('data', deflateOnData);
    }

    this._deflate[kCallback] = callback;

    this._deflate.write(data);
    this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
      if (!this._deflate) {
        //
        // The deflate stream was closed while data was being processed.
        //
        return;
      }

      let data = bufferUtil.concat(
        this._deflate[kBuffers],
        this._deflate[kTotalLength]
      );

      if (fin) {
        data = new FastBuffer(data.buffer, data.byteOffset, data.length - 4);
      }

      //
      // Ensure that the callback will not be called again in
      // `PerMessageDeflate#cleanup()`.
      //
      this._deflate[kCallback] = null;

      this._deflate[kTotalLength] = 0;
      this._deflate[kBuffers] = [];

      if (fin && this.params[`${endpoint}_no_context_takeover`]) {
        this._deflate.reset();
      }

      callback(null, data);
    });
  }
}

module.exports = PerMessageDeflate;

/**
 * The listener of the `zlib.DeflateRaw` stream `'data'` event.
 *
 * @param {Buffer} chunk A chunk of data
 * @private
 */
function deflateOnData(chunk) {
  this[kBuffers].push(chunk);
  this[kTotalLength] += chunk.length;
}

/**
 * The listener of the `zlib.InflateRaw` stream `'data'` event.
 *
 * @param {Buffer} chunk A chunk of data
 * @private
 */
function inflateOnData(chunk) {
  this[kTotalLength] += chunk.length;

  if (
    this[kPerMessageDeflate]._maxPayload < 1 ||
    this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload
  ) {
    this[kBuffers].push(chunk);
    return;
  }

  this[kError] = new RangeError('Max payload size exceeded');
  this[kError].code = 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH';
  this[kError][kStatusCode] = 1009;
  this.removeListener('data', inflateOnData);

  //
  // The choice to employ `zlib.reset()` over `zlib.close()` is dictated by the
  // fact that in Node.js versions prior to 13.10.0, the callback for
  // `zlib.flush()` is not called if `zlib.close()` is used. Utilizing
  // `zlib.reset()` ensures that either the callback is invoked or an error is
  // emitted.
  //
  this.reset();
}

/**
 * The listener of the `zlib.InflateRaw` stream `'error'` event.
 *
 * @param {Error} err The emitted error
 * @private
 */
function inflateOnError(err) {
  //
  // There is no need to call `Zlib#close()` as the handle is automatically
  // closed when an error is emitted.
  //
  this[kPerMessageDeflate]._inflate = null;

  if (this[kError]) {
    this[kCallback](this[kError]);
    return;
  }

  err[kStatusCode] = 1007;
  this[kCallback](err);
}


/***/ },

/***/ 6286
(module, __unused_webpack_exports, __webpack_require__) {



const { Writable } = __webpack_require__(2203);

const PerMessageDeflate = __webpack_require__(2971);
const {
  BINARY_TYPES,
  EMPTY_BUFFER,
  kStatusCode,
  kWebSocket
} = __webpack_require__(2614);
const { concat, toArrayBuffer, unmask } = __webpack_require__(3338);
const { isValidStatusCode, isValidUTF8 } = __webpack_require__(5880);

const FastBuffer = Buffer[Symbol.species];

const GET_INFO = 0;
const GET_PAYLOAD_LENGTH_16 = 1;
const GET_PAYLOAD_LENGTH_64 = 2;
const GET_MASK = 3;
const GET_DATA = 4;
const INFLATING = 5;
const DEFER_EVENT = 6;

/**
 * HyBi Receiver implementation.
 *
 * @extends Writable
 */
class Receiver extends Writable {
  /**
   * Creates a Receiver instance.
   *
   * @param {Object} [options] Options object
   * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
   *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
   *     multiple times in the same tick
   * @param {String} [options.binaryType=nodebuffer] The type for binary data
   * @param {Object} [options.extensions] An object containing the negotiated
   *     extensions
   * @param {Boolean} [options.isServer=false] Specifies whether to operate in
   *     client or server mode
   * @param {Number} [options.maxBufferedChunks=0] The maximum number of
   *     buffered data chunks
   * @param {Number} [options.maxFragments=0] The maximum number of message
   *     fragments
   * @param {Number} [options.maxPayload=0] The maximum allowed message length
   * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
   *     not to skip UTF-8 validation for text and close messages
   */
  constructor(options = {}) {
    super();

    this._allowSynchronousEvents =
      options.allowSynchronousEvents !== undefined
        ? options.allowSynchronousEvents
        : true;
    this._binaryType = options.binaryType || BINARY_TYPES[0];
    this._extensions = options.extensions || {};
    this._isServer = !!options.isServer;
    this._maxBufferedChunks = options.maxBufferedChunks | 0;
    this._maxFragments = options.maxFragments | 0;
    this._maxPayload = options.maxPayload | 0;
    this._skipUTF8Validation = !!options.skipUTF8Validation;
    this[kWebSocket] = undefined;

    this._bufferedBytes = 0;
    this._buffers = [];

    this._compressed = false;
    this._payloadLength = 0;
    this._mask = undefined;
    this._fragmented = 0;
    this._masked = false;
    this._fin = false;
    this._opcode = 0;

    this._totalPayloadLength = 0;
    this._messageLength = 0;
    this._numFragments = 0;
    this._fragments = [];

    this._errored = false;
    this._loop = false;
    this._state = GET_INFO;
  }

  /**
   * Implements `Writable.prototype._write()`.
   *
   * @param {Buffer} chunk The chunk of data to write
   * @param {String} encoding The character encoding of `chunk`
   * @param {Function} cb Callback
   * @private
   */
  _write(chunk, encoding, cb) {
    if (this._opcode === 0x08 && this._state == GET_INFO) return cb();

    if (
      this._maxBufferedChunks > 0 &&
      this._buffers.length >= this._maxBufferedChunks
    ) {
      cb(
        this.createError(
          RangeError,
          'Too many buffered chunks',
          false,
          1008,
          'WS_ERR_TOO_MANY_BUFFERED_PARTS'
        )
      );
      return;
    }

    this._bufferedBytes += chunk.length;
    this._buffers.push(chunk);
    this.startLoop(cb);
  }

  /**
   * Consumes `n` bytes from the buffered data.
   *
   * @param {Number} n The number of bytes to consume
   * @return {Buffer} The consumed bytes
   * @private
   */
  consume(n) {
    this._bufferedBytes -= n;

    if (n === this._buffers[0].length) return this._buffers.shift();

    if (n < this._buffers[0].length) {
      const buf = this._buffers[0];
      this._buffers[0] = new FastBuffer(
        buf.buffer,
        buf.byteOffset + n,
        buf.length - n
      );

      return new FastBuffer(buf.buffer, buf.byteOffset, n);
    }

    const dst = Buffer.allocUnsafe(n);

    do {
      const buf = this._buffers[0];
      const offset = dst.length - n;

      if (n >= buf.length) {
        dst.set(this._buffers.shift(), offset);
      } else {
        dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
        this._buffers[0] = new FastBuffer(
          buf.buffer,
          buf.byteOffset + n,
          buf.length - n
        );
      }

      n -= buf.length;
    } while (n > 0);

    return dst;
  }

  /**
   * Starts the parsing loop.
   *
   * @param {Function} cb Callback
   * @private
   */
  startLoop(cb) {
    this._loop = true;

    do {
      switch (this._state) {
        case GET_INFO:
          this.getInfo(cb);
          break;
        case GET_PAYLOAD_LENGTH_16:
          this.getPayloadLength16(cb);
          break;
        case GET_PAYLOAD_LENGTH_64:
          this.getPayloadLength64(cb);
          break;
        case GET_MASK:
          this.getMask();
          break;
        case GET_DATA:
          this.getData(cb);
          break;
        case INFLATING:
        case DEFER_EVENT:
          this._loop = false;
          return;
      }
    } while (this._loop);

    if (!this._errored) cb();
  }

  /**
   * Reads the first two bytes of a frame.
   *
   * @param {Function} cb Callback
   * @private
   */
  getInfo(cb) {
    if (this._bufferedBytes < 2) {
      this._loop = false;
      return;
    }

    const buf = this.consume(2);

    if ((buf[0] & 0x30) !== 0x00) {
      const error = this.createError(
        RangeError,
        'RSV2 and RSV3 must be clear',
        true,
        1002,
        'WS_ERR_UNEXPECTED_RSV_2_3'
      );

      cb(error);
      return;
    }

    const compressed = (buf[0] & 0x40) === 0x40;

    if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
      const error = this.createError(
        RangeError,
        'RSV1 must be clear',
        true,
        1002,
        'WS_ERR_UNEXPECTED_RSV_1'
      );

      cb(error);
      return;
    }

    this._fin = (buf[0] & 0x80) === 0x80;
    this._opcode = buf[0] & 0x0f;
    this._payloadLength = buf[1] & 0x7f;

    if (this._opcode === 0x00) {
      if (compressed) {
        const error = this.createError(
          RangeError,
          'RSV1 must be clear',
          true,
          1002,
          'WS_ERR_UNEXPECTED_RSV_1'
        );

        cb(error);
        return;
      }

      if (!this._fragmented) {
        const error = this.createError(
          RangeError,
          'invalid opcode 0',
          true,
          1002,
          'WS_ERR_INVALID_OPCODE'
        );

        cb(error);
        return;
      }

      this._opcode = this._fragmented;
    } else if (this._opcode === 0x01 || this._opcode === 0x02) {
      if (this._fragmented) {
        const error = this.createError(
          RangeError,
          `invalid opcode ${this._opcode}`,
          true,
          1002,
          'WS_ERR_INVALID_OPCODE'
        );

        cb(error);
        return;
      }

      this._compressed = compressed;
    } else if (this._opcode > 0x07 && this._opcode < 0x0b) {
      if (!this._fin) {
        const error = this.createError(
          RangeError,
          'FIN must be set',
          true,
          1002,
          'WS_ERR_EXPECTED_FIN'
        );

        cb(error);
        return;
      }

      if (compressed) {
        const error = this.createError(
          RangeError,
          'RSV1 must be clear',
          true,
          1002,
          'WS_ERR_UNEXPECTED_RSV_1'
        );

        cb(error);
        return;
      }

      if (
        this._payloadLength > 0x7d ||
        (this._opcode === 0x08 && this._payloadLength === 1)
      ) {
        const error = this.createError(
          RangeError,
          `invalid payload length ${this._payloadLength}`,
          true,
          1002,
          'WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH'
        );

        cb(error);
        return;
      }
    } else {
      const error = this.createError(
        RangeError,
        `invalid opcode ${this._opcode}`,
        true,
        1002,
        'WS_ERR_INVALID_OPCODE'
      );

      cb(error);
      return;
    }

    if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
    this._masked = (buf[1] & 0x80) === 0x80;

    if (this._isServer) {
      if (!this._masked) {
        const error = this.createError(
          RangeError,
          'MASK must be set',
          true,
          1002,
          'WS_ERR_EXPECTED_MASK'
        );

        cb(error);
        return;
      }
    } else if (this._masked) {
      const error = this.createError(
        RangeError,
        'MASK must be clear',
        true,
        1002,
        'WS_ERR_UNEXPECTED_MASK'
      );

      cb(error);
      return;
    }

    if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
    else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
    else this.haveLength(cb);
  }

  /**
   * Gets extended payload length (7+16).
   *
   * @param {Function} cb Callback
   * @private
   */
  getPayloadLength16(cb) {
    if (this._bufferedBytes < 2) {
      this._loop = false;
      return;
    }

    this._payloadLength = this.consume(2).readUInt16BE(0);
    this.haveLength(cb);
  }

  /**
   * Gets extended payload length (7+64).
   *
   * @param {Function} cb Callback
   * @private
   */
  getPayloadLength64(cb) {
    if (this._bufferedBytes < 8) {
      this._loop = false;
      return;
    }

    const buf = this.consume(8);
    const num = buf.readUInt32BE(0);

    //
    // The maximum safe integer in JavaScript is 2^53 - 1. An error is returned
    // if payload length is greater than this number.
    //
    if (num > Math.pow(2, 53 - 32) - 1) {
      const error = this.createError(
        RangeError,
        'Unsupported WebSocket frame: payload length > 2^53 - 1',
        false,
        1009,
        'WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH'
      );

      cb(error);
      return;
    }

    this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
    this.haveLength(cb);
  }

  /**
   * Payload length has been read.
   *
   * @param {Function} cb Callback
   * @private
   */
  haveLength(cb) {
    if (this._payloadLength && this._opcode < 0x08) {
      this._totalPayloadLength += this._payloadLength;
      if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
        const error = this.createError(
          RangeError,
          'Max payload size exceeded',
          false,
          1009,
          'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'
        );

        cb(error);
        return;
      }
    }

    if (this._masked) this._state = GET_MASK;
    else this._state = GET_DATA;
  }

  /**
   * Reads mask bytes.
   *
   * @private
   */
  getMask() {
    if (this._bufferedBytes < 4) {
      this._loop = false;
      return;
    }

    this._mask = this.consume(4);
    this._state = GET_DATA;
  }

  /**
   * Reads data bytes.
   *
   * @param {Function} cb Callback
   * @private
   */
  getData(cb) {
    let data = EMPTY_BUFFER;

    if (this._payloadLength) {
      if (this._bufferedBytes < this._payloadLength) {
        this._loop = false;
        return;
      }

      data = this.consume(this._payloadLength);

      if (
        this._masked &&
        (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0
      ) {
        unmask(data, this._mask);
      }
    }

    if (this._opcode > 0x07) {
      this.controlMessage(data, cb);
      return;
    }

    if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
      const error = this.createError(
        RangeError,
        'Too many message fragments',
        false,
        1008,
        'WS_ERR_TOO_MANY_BUFFERED_PARTS'
      );

      cb(error);
      return;
    }

    if (this._compressed) {
      this._state = INFLATING;
      this.decompress(data, cb);
      return;
    }

    if (data.length) {
      //
      // This message is not compressed so its length is the sum of the payload
      // length of all fragments.
      //
      this._messageLength = this._totalPayloadLength;
      this._fragments.push(data);
    }

    this.dataMessage(cb);
  }

  /**
   * Decompresses data.
   *
   * @param {Buffer} data Compressed data
   * @param {Function} cb Callback
   * @private
   */
  decompress(data, cb) {
    const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];

    perMessageDeflate.decompress(data, this._fin, (err, buf) => {
      if (err) return cb(err);

      if (buf.length) {
        this._messageLength += buf.length;
        if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
          const error = this.createError(
            RangeError,
            'Max payload size exceeded',
            false,
            1009,
            'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'
          );

          cb(error);
          return;
        }

        this._fragments.push(buf);
      }

      this.dataMessage(cb);
      if (this._state === GET_INFO) this.startLoop(cb);
    });
  }

  /**
   * Handles a data message.
   *
   * @param {Function} cb Callback
   * @private
   */
  dataMessage(cb) {
    if (!this._fin) {
      this._state = GET_INFO;
      return;
    }

    const messageLength = this._messageLength;
    const fragments = this._fragments;

    this._totalPayloadLength = 0;
    this._messageLength = 0;
    this._fragmented = 0;
    this._numFragments = 0;
    this._fragments = [];

    if (this._opcode === 2) {
      let data;

      if (this._binaryType === 'nodebuffer') {
        data = concat(fragments, messageLength);
      } else if (this._binaryType === 'arraybuffer') {
        data = toArrayBuffer(concat(fragments, messageLength));
      } else if (this._binaryType === 'blob') {
        data = new Blob(fragments);
      } else {
        data = fragments;
      }

      if (this._allowSynchronousEvents) {
        this.emit('message', data, true);
        this._state = GET_INFO;
      } else {
        this._state = DEFER_EVENT;
        setImmediate(() => {
          this.emit('message', data, true);
          this._state = GET_INFO;
          this.startLoop(cb);
        });
      }
    } else {
      const buf = concat(fragments, messageLength);

      if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
        const error = this.createError(
          Error,
          'invalid UTF-8 sequence',
          true,
          1007,
          'WS_ERR_INVALID_UTF8'
        );

        cb(error);
        return;
      }

      if (this._state === INFLATING || this._allowSynchronousEvents) {
        this.emit('message', buf, false);
        this._state = GET_INFO;
      } else {
        this._state = DEFER_EVENT;
        setImmediate(() => {
          this.emit('message', buf, false);
          this._state = GET_INFO;
          this.startLoop(cb);
        });
      }
    }
  }

  /**
   * Handles a control message.
   *
   * @param {Buffer} data Data to handle
   * @return {(Error|RangeError|undefined)} A possible error
   * @private
   */
  controlMessage(data, cb) {
    if (this._opcode === 0x08) {
      if (data.length === 0) {
        this._loop = false;
        this.emit('conclude', 1005, EMPTY_BUFFER);
        this.end();
      } else {
        const code = data.readUInt16BE(0);

        if (!isValidStatusCode(code)) {
          const error = this.createError(
            RangeError,
            `invalid status code ${code}`,
            true,
            1002,
            'WS_ERR_INVALID_CLOSE_CODE'
          );

          cb(error);
          return;
        }

        const buf = new FastBuffer(
          data.buffer,
          data.byteOffset + 2,
          data.length - 2
        );

        if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
          const error = this.createError(
            Error,
            'invalid UTF-8 sequence',
            true,
            1007,
            'WS_ERR_INVALID_UTF8'
          );

          cb(error);
          return;
        }

        this._loop = false;
        this.emit('conclude', code, buf);
        this.end();
      }

      this._state = GET_INFO;
      return;
    }

    if (this._allowSynchronousEvents) {
      this.emit(this._opcode === 0x09 ? 'ping' : 'pong', data);
      this._state = GET_INFO;
    } else {
      this._state = DEFER_EVENT;
      setImmediate(() => {
        this.emit(this._opcode === 0x09 ? 'ping' : 'pong', data);
        this._state = GET_INFO;
        this.startLoop(cb);
      });
    }
  }

  /**
   * Builds an error object.
   *
   * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
   * @param {String} message The error message
   * @param {Boolean} prefix Specifies whether or not to add a default prefix to
   *     `message`
   * @param {Number} statusCode The status code
   * @param {String} errorCode The exposed error code
   * @return {(Error|RangeError)} The error
   * @private
   */
  createError(ErrorCtor, message, prefix, statusCode, errorCode) {
    this._loop = false;
    this._errored = true;

    const err = new ErrorCtor(
      prefix ? `Invalid WebSocket frame: ${message}` : message
    );

    Error.captureStackTrace(err, this.createError);
    err.code = errorCode;
    err[kStatusCode] = statusCode;
    return err;
  }
}

module.exports = Receiver;


/***/ },

/***/ 914
(module, __unused_webpack_exports, __webpack_require__) {

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^Duplex" }] */



const { Duplex } = __webpack_require__(2203);
const { randomFillSync } = __webpack_require__(6982);
const {
  types: { isUint8Array }
} = __webpack_require__(9023);

const PerMessageDeflate = __webpack_require__(2971);
const { EMPTY_BUFFER, kWebSocket, NOOP } = __webpack_require__(2614);
const { isBlob, isValidStatusCode } = __webpack_require__(5880);
const { mask: applyMask, toBuffer } = __webpack_require__(3338);

const kByteLength = Symbol('kByteLength');
const maskBuffer = Buffer.alloc(4);
const RANDOM_POOL_SIZE = 8 * 1024;
let randomPool;
let randomPoolPointer = RANDOM_POOL_SIZE;

const DEFAULT = 0;
const DEFLATING = 1;
const GET_BLOB_DATA = 2;

/**
 * HyBi Sender implementation.
 */
class Sender {
  /**
   * Creates a Sender instance.
   *
   * @param {Duplex} socket The connection socket
   * @param {Object} [extensions] An object containing the negotiated extensions
   * @param {Function} [generateMask] The function used to generate the masking
   *     key
   */
  constructor(socket, extensions, generateMask) {
    this._extensions = extensions || {};

    if (generateMask) {
      this._generateMask = generateMask;
      this._maskBuffer = Buffer.alloc(4);
    }

    this._socket = socket;

    this._firstFragment = true;
    this._compress = false;

    this._bufferedBytes = 0;
    this._queue = [];
    this._state = DEFAULT;
    this.onerror = NOOP;
    this[kWebSocket] = undefined;
  }

  /**
   * Frames a piece of data according to the HyBi WebSocket protocol.
   *
   * @param {(Buffer|String)} data The data to frame
   * @param {Object} options Options object
   * @param {Boolean} [options.fin=false] Specifies whether or not to set the
   *     FIN bit
   * @param {Function} [options.generateMask] The function used to generate the
   *     masking key
   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
   *     `data`
   * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
   *     key
   * @param {Number} options.opcode The opcode
   * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
   *     modified
   * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
   *     RSV1 bit
   * @return {(Buffer|String)[]} The framed data
   * @public
   */
  static frame(data, options) {
    let mask;
    let merge = false;
    let offset = 2;
    let skipMasking = false;

    if (options.mask) {
      mask = options.maskBuffer || maskBuffer;

      if (options.generateMask) {
        options.generateMask(mask);
      } else {
        if (randomPoolPointer === RANDOM_POOL_SIZE) {
          /* istanbul ignore else  */
          if (randomPool === undefined) {
            //
            // This is lazily initialized because server-sent frames must not
            // be masked so it may never be used.
            //
            randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
          }

          randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
          randomPoolPointer = 0;
        }

        mask[0] = randomPool[randomPoolPointer++];
        mask[1] = randomPool[randomPoolPointer++];
        mask[2] = randomPool[randomPoolPointer++];
        mask[3] = randomPool[randomPoolPointer++];
      }

      skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
      offset = 6;
    }

    let dataLength;

    if (typeof data === 'string') {
      if (
        (!options.mask || skipMasking) &&
        options[kByteLength] !== undefined
      ) {
        dataLength = options[kByteLength];
      } else {
        data = Buffer.from(data);
        dataLength = data.length;
      }
    } else {
      dataLength = data.length;
      merge = options.mask && options.readOnly && !skipMasking;
    }

    let payloadLength = dataLength;

    if (dataLength >= 65536) {
      offset += 8;
      payloadLength = 127;
    } else if (dataLength > 125) {
      offset += 2;
      payloadLength = 126;
    }

    const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);

    target[0] = options.fin ? options.opcode | 0x80 : options.opcode;
    if (options.rsv1) target[0] |= 0x40;

    target[1] = payloadLength;

    if (payloadLength === 126) {
      target.writeUInt16BE(dataLength, 2);
    } else if (payloadLength === 127) {
      target[2] = target[3] = 0;
      target.writeUIntBE(dataLength, 4, 6);
    }

    if (!options.mask) return [target, data];

    target[1] |= 0x80;
    target[offset - 4] = mask[0];
    target[offset - 3] = mask[1];
    target[offset - 2] = mask[2];
    target[offset - 1] = mask[3];

    if (skipMasking) return [target, data];

    if (merge) {
      applyMask(data, mask, target, offset, dataLength);
      return [target];
    }

    applyMask(data, mask, data, 0, dataLength);
    return [target, data];
  }

  /**
   * Sends a close message to the other peer.
   *
   * @param {Number} [code] The status code component of the body
   * @param {(String|Buffer)} [data] The message component of the body
   * @param {Boolean} [mask=false] Specifies whether or not to mask the message
   * @param {Function} [cb] Callback
   * @public
   */
  close(code, data, mask, cb) {
    let buf;

    if (code === undefined) {
      buf = EMPTY_BUFFER;
    } else if (typeof code !== 'number' || !isValidStatusCode(code)) {
      throw new TypeError('First argument must be a valid error code number');
    } else if (data === undefined || !data.length) {
      buf = Buffer.allocUnsafe(2);
      buf.writeUInt16BE(code, 0);
    } else {
      const length = Buffer.byteLength(data);

      if (length > 123) {
        throw new RangeError('The message must not be greater than 123 bytes');
      }

      buf = Buffer.allocUnsafe(2 + length);
      buf.writeUInt16BE(code, 0);

      if (typeof data === 'string') {
        buf.write(data, 2);
      } else if (isUint8Array(data)) {
        buf.set(data, 2);
      } else {
        throw new TypeError('Second argument must be a string or a Uint8Array');
      }
    }

    const options = {
      [kByteLength]: buf.length,
      fin: true,
      generateMask: this._generateMask,
      mask,
      maskBuffer: this._maskBuffer,
      opcode: 0x08,
      readOnly: false,
      rsv1: false
    };

    if (this._state !== DEFAULT) {
      this.enqueue([this.dispatch, buf, false, options, cb]);
    } else {
      this.sendFrame(Sender.frame(buf, options), cb);
    }
  }

  /**
   * Sends a ping message to the other peer.
   *
   * @param {*} data The message to send
   * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
   * @param {Function} [cb] Callback
   * @public
   */
  ping(data, mask, cb) {
    let byteLength;
    let readOnly;

    if (typeof data === 'string') {
      byteLength = Buffer.byteLength(data);
      readOnly = false;
    } else if (isBlob(data)) {
      byteLength = data.size;
      readOnly = false;
    } else {
      data = toBuffer(data);
      byteLength = data.length;
      readOnly = toBuffer.readOnly;
    }

    if (byteLength > 125) {
      throw new RangeError('The data size must not be greater than 125 bytes');
    }

    const options = {
      [kByteLength]: byteLength,
      fin: true,
      generateMask: this._generateMask,
      mask,
      maskBuffer: this._maskBuffer,
      opcode: 0x09,
      readOnly,
      rsv1: false
    };

    if (isBlob(data)) {
      if (this._state !== DEFAULT) {
        this.enqueue([this.getBlobData, data, false, options, cb]);
      } else {
        this.getBlobData(data, false, options, cb);
      }
    } else if (this._state !== DEFAULT) {
      this.enqueue([this.dispatch, data, false, options, cb]);
    } else {
      this.sendFrame(Sender.frame(data, options), cb);
    }
  }

  /**
   * Sends a pong message to the other peer.
   *
   * @param {*} data The message to send
   * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
   * @param {Function} [cb] Callback
   * @public
   */
  pong(data, mask, cb) {
    let byteLength;
    let readOnly;

    if (typeof data === 'string') {
      byteLength = Buffer.byteLength(data);
      readOnly = false;
    } else if (isBlob(data)) {
      byteLength = data.size;
      readOnly = false;
    } else {
      data = toBuffer(data);
      byteLength = data.length;
      readOnly = toBuffer.readOnly;
    }

    if (byteLength > 125) {
      throw new RangeError('The data size must not be greater than 125 bytes');
    }

    const options = {
      [kByteLength]: byteLength,
      fin: true,
      generateMask: this._generateMask,
      mask,
      maskBuffer: this._maskBuffer,
      opcode: 0x0a,
      readOnly,
      rsv1: false
    };

    if (isBlob(data)) {
      if (this._state !== DEFAULT) {
        this.enqueue([this.getBlobData, data, false, options, cb]);
      } else {
        this.getBlobData(data, false, options, cb);
      }
    } else if (this._state !== DEFAULT) {
      this.enqueue([this.dispatch, data, false, options, cb]);
    } else {
      this.sendFrame(Sender.frame(data, options), cb);
    }
  }

  /**
   * Sends a data message to the other peer.
   *
   * @param {*} data The message to send
   * @param {Object} options Options object
   * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
   *     or text
   * @param {Boolean} [options.compress=false] Specifies whether or not to
   *     compress `data`
   * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
   *     last one
   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
   *     `data`
   * @param {Function} [cb] Callback
   * @public
   */
  send(data, options, cb) {
    const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
    let opcode = options.binary ? 2 : 1;
    let rsv1 = options.compress;

    let byteLength;
    let readOnly;

    if (typeof data === 'string') {
      byteLength = Buffer.byteLength(data);
      readOnly = false;
    } else if (isBlob(data)) {
      byteLength = data.size;
      readOnly = false;
    } else {
      data = toBuffer(data);
      byteLength = data.length;
      readOnly = toBuffer.readOnly;
    }

    if (this._firstFragment) {
      this._firstFragment = false;
      if (
        rsv1 &&
        perMessageDeflate &&
        perMessageDeflate.params[
          perMessageDeflate._isServer
            ? 'server_no_context_takeover'
            : 'client_no_context_takeover'
        ]
      ) {
        rsv1 = byteLength >= perMessageDeflate._threshold;
      }
      this._compress = rsv1;
    } else {
      rsv1 = false;
      opcode = 0;
    }

    if (options.fin) this._firstFragment = true;

    const opts = {
      [kByteLength]: byteLength,
      fin: options.fin,
      generateMask: this._generateMask,
      mask: options.mask,
      maskBuffer: this._maskBuffer,
      opcode,
      readOnly,
      rsv1
    };

    if (isBlob(data)) {
      if (this._state !== DEFAULT) {
        this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
      } else {
        this.getBlobData(data, this._compress, opts, cb);
      }
    } else if (this._state !== DEFAULT) {
      this.enqueue([this.dispatch, data, this._compress, opts, cb]);
    } else {
      this.dispatch(data, this._compress, opts, cb);
    }
  }

  /**
   * Gets the contents of a blob as binary data.
   *
   * @param {Blob} blob The blob
   * @param {Boolean} [compress=false] Specifies whether or not to compress
   *     the data
   * @param {Object} options Options object
   * @param {Boolean} [options.fin=false] Specifies whether or not to set the
   *     FIN bit
   * @param {Function} [options.generateMask] The function used to generate the
   *     masking key
   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
   *     `data`
   * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
   *     key
   * @param {Number} options.opcode The opcode
   * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
   *     modified
   * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
   *     RSV1 bit
   * @param {Function} [cb] Callback
   * @private
   */
  getBlobData(blob, compress, options, cb) {
    this._bufferedBytes += options[kByteLength];
    this._state = GET_BLOB_DATA;

    blob
      .arrayBuffer()
      .then((arrayBuffer) => {
        if (this._socket.destroyed) {
          const err = new Error(
            'The socket was closed while the blob was being read'
          );

          //
          // `callCallbacks` is called in the next tick to ensure that errors
          // that might be thrown in the callbacks behave like errors thrown
          // outside the promise chain.
          //
          process.nextTick(callCallbacks, this, err, cb);
          return;
        }

        this._bufferedBytes -= options[kByteLength];
        const data = toBuffer(arrayBuffer);

        if (!compress) {
          this._state = DEFAULT;
          this.sendFrame(Sender.frame(data, options), cb);
          this.dequeue();
        } else {
          this.dispatch(data, compress, options, cb);
        }
      })
      .catch((err) => {
        //
        // `onError` is called in the next tick for the same reason that
        // `callCallbacks` above is.
        //
        process.nextTick(onError, this, err, cb);
      });
  }

  /**
   * Dispatches a message.
   *
   * @param {(Buffer|String)} data The message to send
   * @param {Boolean} [compress=false] Specifies whether or not to compress
   *     `data`
   * @param {Object} options Options object
   * @param {Boolean} [options.fin=false] Specifies whether or not to set the
   *     FIN bit
   * @param {Function} [options.generateMask] The function used to generate the
   *     masking key
   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
   *     `data`
   * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
   *     key
   * @param {Number} options.opcode The opcode
   * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
   *     modified
   * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
   *     RSV1 bit
   * @param {Function} [cb] Callback
   * @private
   */
  dispatch(data, compress, options, cb) {
    if (!compress) {
      this.sendFrame(Sender.frame(data, options), cb);
      return;
    }

    const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];

    this._bufferedBytes += options[kByteLength];
    this._state = DEFLATING;
    perMessageDeflate.compress(data, options.fin, (_, buf) => {
      if (this._socket.destroyed) {
        const err = new Error(
          'The socket was closed while data was being compressed'
        );

        callCallbacks(this, err, cb);
        return;
      }

      this._bufferedBytes -= options[kByteLength];
      this._state = DEFAULT;
      options.readOnly = false;
      this.sendFrame(Sender.frame(buf, options), cb);
      this.dequeue();
    });
  }

  /**
   * Executes queued send operations.
   *
   * @private
   */
  dequeue() {
    while (this._state === DEFAULT && this._queue.length) {
      const params = this._queue.shift();

      this._bufferedBytes -= params[3][kByteLength];
      Reflect.apply(params[0], this, params.slice(1));
    }
  }

  /**
   * Enqueues a send operation.
   *
   * @param {Array} params Send operation parameters.
   * @private
   */
  enqueue(params) {
    this._bufferedBytes += params[3][kByteLength];
    this._queue.push(params);
  }

  /**
   * Sends a frame.
   *
   * @param {(Buffer | String)[]} list The frame to send
   * @param {Function} [cb] Callback
   * @private
   */
  sendFrame(list, cb) {
    if (list.length === 2) {
      this._socket.cork();
      this._socket.write(list[0]);
      this._socket.write(list[1], cb);
      this._socket.uncork();
    } else {
      this._socket.write(list[0], cb);
    }
  }
}

module.exports = Sender;

/**
 * Calls queued callbacks with an error.
 *
 * @param {Sender} sender The `Sender` instance
 * @param {Error} err The error to call the callbacks with
 * @param {Function} [cb] The first callback
 * @private
 */
function callCallbacks(sender, err, cb) {
  if (typeof cb === 'function') cb(err);

  for (let i = 0; i < sender._queue.length; i++) {
    const params = sender._queue[i];
    const callback = params[params.length - 1];

    if (typeof callback === 'function') callback(err);
  }
}

/**
 * Handles a `Sender` error.
 *
 * @param {Sender} sender The `Sender` instance
 * @param {Error} err The error
 * @param {Function} [cb] The first pending callback
 * @private
 */
function onError(sender, err, cb) {
  callCallbacks(sender, err, cb);
  sender.onerror(err);
}


/***/ },

/***/ 3719
(module, __unused_webpack_exports, __webpack_require__) {

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^WebSocket$" }] */


const WebSocket = __webpack_require__(1060);
const { Duplex } = __webpack_require__(2203);

/**
 * Emits the `'close'` event on a stream.
 *
 * @param {Duplex} stream The stream.
 * @private
 */
function emitClose(stream) {
  stream.emit('close');
}

/**
 * The listener of the `'end'` event.
 *
 * @private
 */
function duplexOnEnd() {
  if (!this.destroyed && this._writableState.finished) {
    this.destroy();
  }
}

/**
 * The listener of the `'error'` event.
 *
 * @param {Error} err The error
 * @private
 */
function duplexOnError(err) {
  this.removeListener('error', duplexOnError);
  this.destroy();
  if (this.listenerCount('error') === 0) {
    // Do not suppress the throwing behavior.
    this.emit('error', err);
  }
}

/**
 * Wraps a `WebSocket` in a duplex stream.
 *
 * @param {WebSocket} ws The `WebSocket` to wrap
 * @param {Object} [options] The options for the `Duplex` constructor
 * @return {Duplex} The duplex stream
 * @public
 */
function createWebSocketStream(ws, options) {
  let terminateOnDestroy = true;

  const duplex = new Duplex({
    ...options,
    autoDestroy: false,
    emitClose: false,
    objectMode: false,
    writableObjectMode: false
  });

  ws.on('message', function message(msg, isBinary) {
    const data =
      !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;

    if (!duplex.push(data)) ws.pause();
  });

  ws.once('error', function error(err) {
    if (duplex.destroyed) return;

    // Prevent `ws.terminate()` from being called by `duplex._destroy()`.
    //
    // - If the `'error'` event is emitted before the `'open'` event, then
    //   `ws.terminate()` is a noop as no socket is assigned.
    // - Otherwise, the error is re-emitted by the listener of the `'error'`
    //   event of the `Receiver` object. The listener already closes the
    //   connection by calling `ws.close()`. This allows a close frame to be
    //   sent to the other peer. If `ws.terminate()` is called right after this,
    //   then the close frame might not be sent.
    terminateOnDestroy = false;
    duplex.destroy(err);
  });

  ws.once('close', function close() {
    if (duplex.destroyed) return;

    duplex.push(null);
  });

  duplex._destroy = function (err, callback) {
    if (ws.readyState === ws.CLOSED) {
      callback(err);
      process.nextTick(emitClose, duplex);
      return;
    }

    let called = false;

    ws.once('error', function error(err) {
      called = true;
      callback(err);
    });

    ws.once('close', function close() {
      if (!called) callback(err);
      process.nextTick(emitClose, duplex);
    });

    if (terminateOnDestroy) ws.terminate();
  };

  duplex._final = function (callback) {
    if (ws.readyState === ws.CONNECTING) {
      ws.once('open', function open() {
        duplex._final(callback);
      });
      return;
    }

    // If the value of the `_socket` property is `null` it means that `ws` is a
    // client websocket and the handshake failed. In fact, when this happens, a
    // socket is never assigned to the websocket. Wait for the `'error'` event
    // that will be emitted by the websocket.
    if (ws._socket === null) return;

    if (ws._socket._writableState.finished) {
      callback();
      if (duplex._readableState.endEmitted) duplex.destroy();
    } else {
      ws._socket.once('finish', function finish() {
        // `duplex` is not destroyed here because the `'end'` event will be
        // emitted on `duplex` after this `'finish'` event. The EOF signaling
        // `null` chunk is, in fact, pushed when the websocket emits `'close'`.
        callback();
      });
      ws.close();
    }
  };

  duplex._read = function () {
    if (ws.isPaused) ws.resume();
  };

  duplex._write = function (chunk, encoding, callback) {
    if (ws.readyState === ws.CONNECTING) {
      ws.once('open', function open() {
        duplex._write(chunk, encoding, callback);
      });
      return;
    }

    ws.send(chunk, callback);
  };

  duplex.on('end', duplexOnEnd);
  duplex.on('error', duplexOnError);
  return duplex;
}

module.exports = createWebSocketStream;


/***/ },

/***/ 8237
(module, __unused_webpack_exports, __webpack_require__) {



const { tokenChars } = __webpack_require__(5880);

/**
 * Parses the `Sec-WebSocket-Protocol` header into a set of subprotocol names.
 *
 * @param {String} header The field value of the header
 * @return {Set} The subprotocol names
 * @public
 */
function parse(header) {
  const protocols = new Set();
  let start = -1;
  let end = -1;
  let i = 0;

  for (i; i < header.length; i++) {
    const code = header.charCodeAt(i);

    if (end === -1 && tokenChars[code] === 1) {
      if (start === -1) start = i;
    } else if (
      i !== 0 &&
      (code === 0x20 /* ' ' */ || code === 0x09) /* '\t' */
    ) {
      if (end === -1 && start !== -1) end = i;
    } else if (code === 0x2c /* ',' */) {
      if (start === -1) {
        throw new SyntaxError(`Unexpected character at index ${i}`);
      }

      if (end === -1) end = i;

      const protocol = header.slice(start, end);

      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }

      protocols.add(protocol);
      start = end = -1;
    } else {
      throw new SyntaxError(`Unexpected character at index ${i}`);
    }
  }

  if (start === -1 || end !== -1) {
    throw new SyntaxError('Unexpected end of input');
  }

  const protocol = header.slice(start, i);

  if (protocols.has(protocol)) {
    throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
  }

  protocols.add(protocol);
  return protocols;
}

module.exports = { parse };


/***/ },

/***/ 5880
(module, __unused_webpack_exports, __webpack_require__) {



const { isUtf8 } = __webpack_require__(181);

const { hasBlob } = __webpack_require__(2614);

//
// Allowed token characters:
//
// '!', '#', '$', '%', '&', ''', '*', '+', '-',
// '.', 0-9, A-Z, '^', '_', '`', a-z, '|', '~'
//
// tokenChars[32] === 0 // ' '
// tokenChars[33] === 1 // '!'
// tokenChars[34] === 0 // '"'
// ...
//
// prettier-ignore
const tokenChars = [
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0 - 15
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 16 - 31
  0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 1, 0, // 32 - 47
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, // 48 - 63
  0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 64 - 79
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, // 80 - 95
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 96 - 111
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0 // 112 - 127
];

/**
 * Checks if a status code is allowed in a close frame.
 *
 * @param {Number} code The status code
 * @return {Boolean} `true` if the status code is valid, else `false`
 * @public
 */
function isValidStatusCode(code) {
  return (
    (code >= 1000 &&
      code <= 1014 &&
      code !== 1004 &&
      code !== 1005 &&
      code !== 1006) ||
    (code >= 3000 && code <= 4999)
  );
}

/**
 * Checks if a given buffer contains only correct UTF-8.
 * Ported from https://www.cl.cam.ac.uk/%7Emgk25/ucs/utf8_check.c by
 * Markus Kuhn.
 *
 * @param {Buffer} buf The buffer to check
 * @return {Boolean} `true` if `buf` contains only correct UTF-8, else `false`
 * @public
 */
function _isValidUTF8(buf) {
  const len = buf.length;
  let i = 0;

  while (i < len) {
    if ((buf[i] & 0x80) === 0) {
      // 0xxxxxxx
      i++;
    } else if ((buf[i] & 0xe0) === 0xc0) {
      // 110xxxxx 10xxxxxx
      if (
        i + 1 === len ||
        (buf[i + 1] & 0xc0) !== 0x80 ||
        (buf[i] & 0xfe) === 0xc0 // Overlong
      ) {
        return false;
      }

      i += 2;
    } else if ((buf[i] & 0xf0) === 0xe0) {
      // 1110xxxx 10xxxxxx 10xxxxxx
      if (
        i + 2 >= len ||
        (buf[i + 1] & 0xc0) !== 0x80 ||
        (buf[i + 2] & 0xc0) !== 0x80 ||
        (buf[i] === 0xe0 && (buf[i + 1] & 0xe0) === 0x80) || // Overlong
        (buf[i] === 0xed && (buf[i + 1] & 0xe0) === 0xa0) // Surrogate (U+D800 - U+DFFF)
      ) {
        return false;
      }

      i += 3;
    } else if ((buf[i] & 0xf8) === 0xf0) {
      // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
      if (
        i + 3 >= len ||
        (buf[i + 1] & 0xc0) !== 0x80 ||
        (buf[i + 2] & 0xc0) !== 0x80 ||
        (buf[i + 3] & 0xc0) !== 0x80 ||
        (buf[i] === 0xf0 && (buf[i + 1] & 0xf0) === 0x80) || // Overlong
        (buf[i] === 0xf4 && buf[i + 1] > 0x8f) ||
        buf[i] > 0xf4 // > U+10FFFF
      ) {
        return false;
      }

      i += 4;
    } else {
      return false;
    }
  }

  return true;
}

/**
 * Determines whether a value is a `Blob`.
 *
 * @param {*} value The value to be tested
 * @return {Boolean} `true` if `value` is a `Blob`, else `false`
 * @private
 */
function isBlob(value) {
  return (
    hasBlob &&
    typeof value === 'object' &&
    typeof value.arrayBuffer === 'function' &&
    typeof value.type === 'string' &&
    typeof value.stream === 'function' &&
    (value[Symbol.toStringTag] === 'Blob' ||
      value[Symbol.toStringTag] === 'File')
  );
}

module.exports = {
  isBlob,
  isValidStatusCode,
  isValidUTF8: _isValidUTF8,
  tokenChars
};

if (isUtf8) {
  module.exports.isValidUTF8 = function (buf) {
    return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
  };
} /* istanbul ignore else  */ else if (!process.env.WS_NO_UTF_8_VALIDATE) {
  try {
    const isValidUTF8 = __webpack_require__(Object(function webpackMissingModule() { const e = new Error("Cannot find module 'utf-8-validate'"); e.code = 'MODULE_NOT_FOUND'; throw e; }()));

    module.exports.isValidUTF8 = function (buf) {
      return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
    };
  } catch (e) {
    // Continue regardless of the error.
  }
}


/***/ },

/***/ 1722
(module, __unused_webpack_exports, __webpack_require__) {

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^Duplex$", "caughtErrors": "none" }] */



const EventEmitter = __webpack_require__(4434);
const http = __webpack_require__(8611);
const { Duplex } = __webpack_require__(2203);
const { createHash } = __webpack_require__(6982);

const extension = __webpack_require__(5926);
const PerMessageDeflate = __webpack_require__(2971);
const subprotocol = __webpack_require__(8237);
const WebSocket = __webpack_require__(1060);
const { CLOSE_TIMEOUT, GUID, kWebSocket } = __webpack_require__(2614);

const keyRegex = /^[+/0-9A-Za-z]{22}==$/;

const RUNNING = 0;
const CLOSING = 1;
const CLOSED = 2;

/**
 * Class representing a WebSocket server.
 *
 * @extends EventEmitter
 */
class WebSocketServer extends EventEmitter {
  /**
   * Create a `WebSocketServer` instance.
   *
   * @param {Object} options Configuration options
   * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
   *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
   *     multiple times in the same tick
   * @param {Boolean} [options.autoPong=true] Specifies whether or not to
   *     automatically send a pong in response to a ping
   * @param {Number} [options.backlog=511] The maximum length of the queue of
   *     pending connections
   * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
   *     track clients
   * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
   *     wait for the closing handshake to finish after `websocket.close()` is
   *     called
   * @param {Function} [options.handleProtocols] A hook to handle protocols
   * @param {String} [options.host] The hostname where to bind the server
   * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
   *     buffered data chunks
   * @param {Number} [options.maxFragments=16384] The maximum number of message
   *     fragments
   * @param {Number} [options.maxPayload=104857600] The maximum allowed message
   *     size
   * @param {Boolean} [options.noServer=false] Enable no server mode
   * @param {String} [options.path] Accept only connections matching this path
   * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
   *     permessage-deflate
   * @param {Number} [options.port] The port where to bind the server
   * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
   *     server to use
   * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
   *     not to skip UTF-8 validation for text and close messages
   * @param {Function} [options.verifyClient] A hook to reject connections
   * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
   *     class to use. It must be the `WebSocket` class or class that extends it
   * @param {Function} [callback] A listener for the `listening` event
   */
  constructor(options, callback) {
    super();

    options = {
      allowSynchronousEvents: true,
      autoPong: true,
      maxBufferedChunks: 256 * 1024,
      maxFragments: 16 * 1024,
      maxPayload: 100 * 1024 * 1024,
      skipUTF8Validation: false,
      perMessageDeflate: false,
      handleProtocols: null,
      clientTracking: true,
      closeTimeout: CLOSE_TIMEOUT,
      verifyClient: null,
      noServer: false,
      backlog: null, // use default (511 as implemented in net.js)
      server: null,
      host: null,
      path: null,
      port: null,
      WebSocket,
      ...options
    };

    if (
      (options.port == null && !options.server && !options.noServer) ||
      (options.port != null && (options.server || options.noServer)) ||
      (options.server && options.noServer)
    ) {
      throw new TypeError(
        'One and only one of the "port", "server", or "noServer" options ' +
          'must be specified'
      );
    }

    if (options.port != null) {
      this._server = http.createServer((req, res) => {
        const body = http.STATUS_CODES[426];

        res.writeHead(426, {
          'Content-Length': body.length,
          'Content-Type': 'text/plain'
        });
        res.end(body);
      });
      this._server.listen(
        options.port,
        options.host,
        options.backlog,
        callback
      );
    } else if (options.server) {
      this._server = options.server;
    }

    if (this._server) {
      const emitConnection = this.emit.bind(this, 'connection');

      this._removeListeners = addListeners(this._server, {
        listening: this.emit.bind(this, 'listening'),
        error: this.emit.bind(this, 'error'),
        upgrade: (req, socket, head) => {
          this.handleUpgrade(req, socket, head, emitConnection);
        }
      });
    }

    if (options.perMessageDeflate === true) options.perMessageDeflate = {};
    if (options.clientTracking) {
      this.clients = new Set();
      this._shouldEmitClose = false;
    }

    this.options = options;
    this._state = RUNNING;
  }

  /**
   * Returns the bound address, the address family name, and port of the server
   * as reported by the operating system if listening on an IP socket.
   * If the server is listening on a pipe or UNIX domain socket, the name is
   * returned as a string.
   *
   * @return {(Object|String|null)} The address of the server
   * @public
   */
  address() {
    if (this.options.noServer) {
      throw new Error('The server is operating in "noServer" mode');
    }

    if (!this._server) return null;
    return this._server.address();
  }

  /**
   * Stop the server from accepting new connections and emit the `'close'` event
   * when all existing connections are closed.
   *
   * @param {Function} [cb] A one-time listener for the `'close'` event
   * @public
   */
  close(cb) {
    if (this._state === CLOSED) {
      if (cb) {
        this.once('close', () => {
          cb(new Error('The server is not running'));
        });
      }

      process.nextTick(emitClose, this);
      return;
    }

    if (cb) this.once('close', cb);

    if (this._state === CLOSING) return;
    this._state = CLOSING;

    if (this.options.noServer || this.options.server) {
      if (this._server) {
        this._removeListeners();
        this._removeListeners = this._server = null;
      }

      if (this.clients) {
        if (!this.clients.size) {
          process.nextTick(emitClose, this);
        } else {
          this._shouldEmitClose = true;
        }
      } else {
        process.nextTick(emitClose, this);
      }
    } else {
      const server = this._server;

      this._removeListeners();
      this._removeListeners = this._server = null;

      //
      // The HTTP/S server was created internally. Close it, and rely on its
      // `'close'` event.
      //
      server.close(() => {
        emitClose(this);
      });
    }
  }

  /**
   * See if a given request should be handled by this server instance.
   *
   * @param {http.IncomingMessage} req Request object to inspect
   * @return {Boolean} `true` if the request is valid, else `false`
   * @public
   */
  shouldHandle(req) {
    if (this.options.path) {
      const index = req.url.indexOf('?');
      const pathname = index !== -1 ? req.url.slice(0, index) : req.url;

      if (pathname !== this.options.path) return false;
    }

    return true;
  }

  /**
   * Handle a HTTP Upgrade request.
   *
   * @param {http.IncomingMessage} req The request object
   * @param {Duplex} socket The network socket between the server and client
   * @param {Buffer} head The first packet of the upgraded stream
   * @param {Function} cb Callback
   * @public
   */
  handleUpgrade(req, socket, head, cb) {
    socket.on('error', socketOnError);

    const key = req.headers['sec-websocket-key'];
    const upgrade = req.headers.upgrade;
    const version = +req.headers['sec-websocket-version'];

    if (req.method !== 'GET') {
      const message = 'Invalid HTTP method';
      abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
      return;
    }

    if (upgrade === undefined || upgrade.toLowerCase() !== 'websocket') {
      const message = 'Invalid Upgrade header';
      abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
      return;
    }

    if (key === undefined || !keyRegex.test(key)) {
      const message = 'Missing or invalid Sec-WebSocket-Key header';
      abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
      return;
    }

    if (version !== 13 && version !== 8) {
      const message = 'Missing or invalid Sec-WebSocket-Version header';
      abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
        'Sec-WebSocket-Version': '13, 8'
      });
      return;
    }

    if (!this.shouldHandle(req)) {
      abortHandshake(socket, 400);
      return;
    }

    const secWebSocketProtocol = req.headers['sec-websocket-protocol'];
    let protocols = new Set();

    if (secWebSocketProtocol !== undefined) {
      try {
        protocols = subprotocol.parse(secWebSocketProtocol);
      } catch (err) {
        const message = 'Invalid Sec-WebSocket-Protocol header';
        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
        return;
      }
    }

    const secWebSocketExtensions = req.headers['sec-websocket-extensions'];
    const extensions = {};

    if (
      this.options.perMessageDeflate &&
      secWebSocketExtensions !== undefined
    ) {
      const perMessageDeflate = new PerMessageDeflate({
        ...this.options.perMessageDeflate,
        isServer: true,
        maxPayload: this.options.maxPayload
      });

      try {
        const offers = extension.parse(secWebSocketExtensions);

        if (offers[PerMessageDeflate.extensionName]) {
          perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
          extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
        }
      } catch (err) {
        const message =
          'Invalid or unacceptable Sec-WebSocket-Extensions header';
        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
        return;
      }
    }

    //
    // Optionally call external client verification handler.
    //
    if (this.options.verifyClient) {
      const info = {
        origin:
          req.headers[`${version === 8 ? 'sec-websocket-origin' : 'origin'}`],
        secure: !!(req.socket.authorized || req.socket.encrypted),
        req
      };

      if (this.options.verifyClient.length === 2) {
        this.options.verifyClient(info, (verified, code, message, headers) => {
          if (!verified) {
            return abortHandshake(socket, code || 401, message, headers);
          }

          this.completeUpgrade(
            extensions,
            key,
            protocols,
            req,
            socket,
            head,
            cb
          );
        });
        return;
      }

      if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
    }

    this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
  }

  /**
   * Upgrade the connection to WebSocket.
   *
   * @param {Object} extensions The accepted extensions
   * @param {String} key The value of the `Sec-WebSocket-Key` header
   * @param {Set} protocols The subprotocols
   * @param {http.IncomingMessage} req The request object
   * @param {Duplex} socket The network socket between the server and client
   * @param {Buffer} head The first packet of the upgraded stream
   * @param {Function} cb Callback
   * @throws {Error} If called more than once with the same socket
   * @private
   */
  completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
    //
    // Destroy the socket if the client has already sent a FIN packet.
    //
    if (!socket.readable || !socket.writable) return socket.destroy();

    if (socket[kWebSocket]) {
      throw new Error(
        'server.handleUpgrade() was called more than once with the same ' +
          'socket, possibly due to a misconfiguration'
      );
    }

    if (this._state > RUNNING) return abortHandshake(socket, 503);

    const digest = createHash('sha1')
      .update(key + GUID)
      .digest('base64');

    const headers = [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${digest}`
    ];

    const ws = new this.options.WebSocket(null, undefined, this.options);

    if (protocols.size) {
      //
      // Optionally call external protocol selection handler.
      //
      const protocol = this.options.handleProtocols
        ? this.options.handleProtocols(protocols, req)
        : protocols.values().next().value;

      if (protocol) {
        headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
        ws._protocol = protocol;
      }
    }

    if (extensions[PerMessageDeflate.extensionName]) {
      const params = extensions[PerMessageDeflate.extensionName].params;
      const value = extension.format({
        [PerMessageDeflate.extensionName]: [params]
      });
      headers.push(`Sec-WebSocket-Extensions: ${value}`);
      ws._extensions = extensions;
    }

    //
    // Allow external modification/inspection of handshake headers.
    //
    this.emit('headers', headers, req);

    socket.write(headers.concat('\r\n').join('\r\n'));
    socket.removeListener('error', socketOnError);

    ws.setSocket(socket, head, {
      allowSynchronousEvents: this.options.allowSynchronousEvents,
      maxBufferedChunks: this.options.maxBufferedChunks,
      maxFragments: this.options.maxFragments,
      maxPayload: this.options.maxPayload,
      skipUTF8Validation: this.options.skipUTF8Validation
    });

    if (this.clients) {
      this.clients.add(ws);
      ws.on('close', () => {
        this.clients.delete(ws);

        if (this._shouldEmitClose && !this.clients.size) {
          process.nextTick(emitClose, this);
        }
      });
    }

    cb(ws, req);
  }
}

module.exports = WebSocketServer;

/**
 * Add event listeners on an `EventEmitter` using a map of <event, listener>
 * pairs.
 *
 * @param {EventEmitter} server The event emitter
 * @param {Object.<String, Function>} map The listeners to add
 * @return {Function} A function that will remove the added listeners when
 *     called
 * @private
 */
function addListeners(server, map) {
  for (const event of Object.keys(map)) server.on(event, map[event]);

  return function removeListeners() {
    for (const event of Object.keys(map)) {
      server.removeListener(event, map[event]);
    }
  };
}

/**
 * Emit a `'close'` event on an `EventEmitter`.
 *
 * @param {EventEmitter} server The event emitter
 * @private
 */
function emitClose(server) {
  server._state = CLOSED;
  server.emit('close');
}

/**
 * Handle socket errors.
 *
 * @private
 */
function socketOnError() {
  this.destroy();
}

/**
 * Close the connection when preconditions are not fulfilled.
 *
 * @param {Duplex} socket The socket of the upgrade request
 * @param {Number} code The HTTP response status code
 * @param {String} [message] The HTTP response body
 * @param {Object} [headers] Additional HTTP response headers
 * @private
 */
function abortHandshake(socket, code, message, headers) {
  //
  // The socket is writable unless the user destroyed or ended it before calling
  // `server.handleUpgrade()` or in the `verifyClient` function, which is a user
  // error. Handling this does not make much sense as the worst that can happen
  // is that some of the data written by the user might be discarded due to the
  // call to `socket.end()` below, which triggers an `'error'` event that in
  // turn causes the socket to be destroyed.
  //
  message = message || http.STATUS_CODES[code];
  headers = {
    Connection: 'close',
    'Content-Type': 'text/html',
    'Content-Length': Buffer.byteLength(message),
    ...headers
  };

  socket.once('finish', socket.destroy);

  socket.end(
    `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r\n` +
      Object.keys(headers)
        .map((h) => `${h}: ${headers[h]}`)
        .join('\r\n') +
      '\r\n\r\n' +
      message
  );
}

/**
 * Emit a `'wsClientError'` event on a `WebSocketServer` if there is at least
 * one listener for it, otherwise call `abortHandshake()`.
 *
 * @param {WebSocketServer} server The WebSocket server
 * @param {http.IncomingMessage} req The request object
 * @param {Duplex} socket The socket of the upgrade request
 * @param {Number} code The HTTP response status code
 * @param {String} message The HTTP response body
 * @param {Object} [headers] The HTTP response headers
 * @private
 */
function abortHandshakeOrEmitwsClientError(
  server,
  req,
  socket,
  code,
  message,
  headers
) {
  if (server.listenerCount('wsClientError')) {
    const err = new Error(message);
    Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);

    server.emit('wsClientError', err, socket, req);
  } else {
    abortHandshake(socket, code, message, headers);
  }
}


/***/ },

/***/ 1060
(module, __unused_webpack_exports, __webpack_require__) {

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^Duplex|Readable$", "caughtErrors": "none" }] */



const EventEmitter = __webpack_require__(4434);
const https = __webpack_require__(5692);
const http = __webpack_require__(8611);
const net = __webpack_require__(9278);
const tls = __webpack_require__(4756);
const { randomBytes, createHash } = __webpack_require__(6982);
const { Duplex, Readable } = __webpack_require__(2203);
const { URL } = __webpack_require__(7016);

const PerMessageDeflate = __webpack_require__(2971);
const Receiver = __webpack_require__(6286);
const Sender = __webpack_require__(914);
const { isBlob } = __webpack_require__(5880);

const {
  BINARY_TYPES,
  CLOSE_TIMEOUT,
  EMPTY_BUFFER,
  GUID,
  kForOnEventAttribute,
  kListener,
  kStatusCode,
  kWebSocket,
  NOOP
} = __webpack_require__(2614);
const {
  EventTarget: { addEventListener, removeEventListener }
} = __webpack_require__(597);
const { format, parse } = __webpack_require__(5926);
const { toBuffer } = __webpack_require__(3338);

const kAborted = Symbol('kAborted');
const protocolVersions = [8, 13];
const readyStates = ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'];
const subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;

/**
 * Class representing a WebSocket.
 *
 * @extends EventEmitter
 */
class WebSocket extends EventEmitter {
  /**
   * Create a new `WebSocket`.
   *
   * @param {(String|URL)} address The URL to which to connect
   * @param {(String|String[])} [protocols] The subprotocols
   * @param {Object} [options] Connection options
   */
  constructor(address, protocols, options) {
    super();

    this._binaryType = BINARY_TYPES[0];
    this._closeCode = 1006;
    this._closeFrameReceived = false;
    this._closeFrameSent = false;
    this._closeMessage = EMPTY_BUFFER;
    this._closeTimer = null;
    this._errorEmitted = false;
    this._extensions = {};
    this._paused = false;
    this._protocol = '';
    this._readyState = WebSocket.CONNECTING;
    this._receiver = null;
    this._sender = null;
    this._socket = null;

    if (address !== null) {
      this._bufferedAmount = 0;
      this._isServer = false;
      this._redirects = 0;

      if (protocols === undefined) {
        protocols = [];
      } else if (!Array.isArray(protocols)) {
        if (typeof protocols === 'object' && protocols !== null) {
          options = protocols;
          protocols = [];
        } else {
          protocols = [protocols];
        }
      }

      initAsClient(this, address, protocols, options);
    } else {
      this._autoPong = options.autoPong;
      this._closeTimeout = options.closeTimeout;
      this._isServer = true;
    }
  }

  /**
   * For historical reasons, the custom "nodebuffer" type is used by the default
   * instead of "blob".
   *
   * @type {String}
   */
  get binaryType() {
    return this._binaryType;
  }

  set binaryType(type) {
    if (!BINARY_TYPES.includes(type)) return;

    this._binaryType = type;

    //
    // Allow to change `binaryType` on the fly.
    //
    if (this._receiver) this._receiver._binaryType = type;
  }

  /**
   * @type {Number}
   */
  get bufferedAmount() {
    if (!this._socket) return this._bufferedAmount;

    return this._socket._writableState.length + this._sender._bufferedBytes;
  }

  /**
   * @type {String}
   */
  get extensions() {
    return Object.keys(this._extensions).join();
  }

  /**
   * @type {Boolean}
   */
  get isPaused() {
    return this._paused;
  }

  /**
   * @type {Function}
   */
  /* istanbul ignore next */
  get onclose() {
    return null;
  }

  /**
   * @type {Function}
   */
  /* istanbul ignore next */
  get onerror() {
    return null;
  }

  /**
   * @type {Function}
   */
  /* istanbul ignore next */
  get onopen() {
    return null;
  }

  /**
   * @type {Function}
   */
  /* istanbul ignore next */
  get onmessage() {
    return null;
  }

  /**
   * @type {String}
   */
  get protocol() {
    return this._protocol;
  }

  /**
   * @type {Number}
   */
  get readyState() {
    return this._readyState;
  }

  /**
   * @type {String}
   */
  get url() {
    return this._url;
  }

  /**
   * Set up the socket and the internal resources.
   *
   * @param {Duplex} socket The network socket between the server and client
   * @param {Buffer} head The first packet of the upgraded stream
   * @param {Object} options Options object
   * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
   *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
   *     multiple times in the same tick
   * @param {Function} [options.generateMask] The function used to generate the
   *     masking key
   * @param {Number} [options.maxBufferedChunks=0] The maximum number of
   *     buffered data chunks
   * @param {Number} [options.maxFragments=0] The maximum number of message
   *     fragments
   * @param {Number} [options.maxPayload=0] The maximum allowed message size
   * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
   *     not to skip UTF-8 validation for text and close messages
   * @private
   */
  setSocket(socket, head, options) {
    const receiver = new Receiver({
      allowSynchronousEvents: options.allowSynchronousEvents,
      binaryType: this.binaryType,
      extensions: this._extensions,
      isServer: this._isServer,
      maxBufferedChunks: options.maxBufferedChunks,
      maxFragments: options.maxFragments,
      maxPayload: options.maxPayload,
      skipUTF8Validation: options.skipUTF8Validation
    });

    const sender = new Sender(socket, this._extensions, options.generateMask);

    this._receiver = receiver;
    this._sender = sender;
    this._socket = socket;

    receiver[kWebSocket] = this;
    sender[kWebSocket] = this;
    socket[kWebSocket] = this;

    receiver.on('conclude', receiverOnConclude);
    receiver.on('drain', receiverOnDrain);
    receiver.on('error', receiverOnError);
    receiver.on('message', receiverOnMessage);
    receiver.on('ping', receiverOnPing);
    receiver.on('pong', receiverOnPong);

    sender.onerror = senderOnError;

    //
    // These methods may not be available if `socket` is just a `Duplex`.
    //
    if (socket.setTimeout) socket.setTimeout(0);
    if (socket.setNoDelay) socket.setNoDelay();

    if (head.length > 0) socket.unshift(head);

    socket.on('close', socketOnClose);
    socket.on('data', socketOnData);
    socket.on('end', socketOnEnd);
    socket.on('error', socketOnError);

    this._readyState = WebSocket.OPEN;
    this.emit('open');
  }

  /**
   * Emit the `'close'` event.
   *
   * @private
   */
  emitClose() {
    if (!this._socket) {
      this._readyState = WebSocket.CLOSED;
      this.emit('close', this._closeCode, this._closeMessage);
      return;
    }

    if (this._extensions[PerMessageDeflate.extensionName]) {
      this._extensions[PerMessageDeflate.extensionName].cleanup();
    }

    this._receiver.removeAllListeners();
    this._readyState = WebSocket.CLOSED;
    this.emit('close', this._closeCode, this._closeMessage);
  }

  /**
   * Start a closing handshake.
   *
   *          +----------+   +-----------+   +----------+
   *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
   *    |     +----------+   +-----------+   +----------+     |
   *          +----------+   +-----------+         |
   * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
   *          +----------+   +-----------+   |
   *    |           |                        |   +---+        |
   *                +------------------------+-->|fin| - - - -
   *    |         +---+                      |   +---+
   *     - - - - -|fin|<---------------------+
   *              +---+
   *
   * @param {Number} [code] Status code explaining why the connection is closing
   * @param {(String|Buffer)} [data] The reason why the connection is
   *     closing
   * @public
   */
  close(code, data) {
    if (this.readyState === WebSocket.CLOSED) return;
    if (this.readyState === WebSocket.CONNECTING) {
      const msg = 'WebSocket was closed before the connection was established';
      abortHandshake(this, this._req, msg);
      return;
    }

    if (this.readyState === WebSocket.CLOSING) {
      if (
        this._closeFrameSent &&
        (this._closeFrameReceived || this._receiver._writableState.errorEmitted)
      ) {
        this._socket.end();
      }

      return;
    }

    this._readyState = WebSocket.CLOSING;
    this._sender.close(code, data, !this._isServer, (err) => {
      //
      // This error is handled by the `'error'` listener on the socket. We only
      // want to know if the close frame has been sent here.
      //
      if (err) return;

      this._closeFrameSent = true;

      if (
        this._closeFrameReceived ||
        this._receiver._writableState.errorEmitted
      ) {
        this._socket.end();
      }
    });

    setCloseTimer(this);
  }

  /**
   * Pause the socket.
   *
   * @public
   */
  pause() {
    if (
      this.readyState === WebSocket.CONNECTING ||
      this.readyState === WebSocket.CLOSED
    ) {
      return;
    }

    this._paused = true;
    this._socket.pause();
  }

  /**
   * Send a ping.
   *
   * @param {*} [data] The data to send
   * @param {Boolean} [mask] Indicates whether or not to mask `data`
   * @param {Function} [cb] Callback which is executed when the ping is sent
   * @public
   */
  ping(data, mask, cb) {
    if (this.readyState === WebSocket.CONNECTING) {
      throw new Error('WebSocket is not open: readyState 0 (CONNECTING)');
    }

    if (typeof data === 'function') {
      cb = data;
      data = mask = undefined;
    } else if (typeof mask === 'function') {
      cb = mask;
      mask = undefined;
    }

    if (typeof data === 'number') data = data.toString();

    if (this.readyState !== WebSocket.OPEN) {
      sendAfterClose(this, data, cb);
      return;
    }

    if (mask === undefined) mask = !this._isServer;
    this._sender.ping(data || EMPTY_BUFFER, mask, cb);
  }

  /**
   * Send a pong.
   *
   * @param {*} [data] The data to send
   * @param {Boolean} [mask] Indicates whether or not to mask `data`
   * @param {Function} [cb] Callback which is executed when the pong is sent
   * @public
   */
  pong(data, mask, cb) {
    if (this.readyState === WebSocket.CONNECTING) {
      throw new Error('WebSocket is not open: readyState 0 (CONNECTING)');
    }

    if (typeof data === 'function') {
      cb = data;
      data = mask = undefined;
    } else if (typeof mask === 'function') {
      cb = mask;
      mask = undefined;
    }

    if (typeof data === 'number') data = data.toString();

    if (this.readyState !== WebSocket.OPEN) {
      sendAfterClose(this, data, cb);
      return;
    }

    if (mask === undefined) mask = !this._isServer;
    this._sender.pong(data || EMPTY_BUFFER, mask, cb);
  }

  /**
   * Resume the socket.
   *
   * @public
   */
  resume() {
    if (
      this.readyState === WebSocket.CONNECTING ||
      this.readyState === WebSocket.CLOSED
    ) {
      return;
    }

    this._paused = false;
    if (!this._receiver._writableState.needDrain) this._socket.resume();
  }

  /**
   * Send a data message.
   *
   * @param {*} data The message to send
   * @param {Object} [options] Options object
   * @param {Boolean} [options.binary] Specifies whether `data` is binary or
   *     text
   * @param {Boolean} [options.compress] Specifies whether or not to compress
   *     `data`
   * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
   *     last one
   * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
   * @param {Function} [cb] Callback which is executed when data is written out
   * @public
   */
  send(data, options, cb) {
    if (this.readyState === WebSocket.CONNECTING) {
      throw new Error('WebSocket is not open: readyState 0 (CONNECTING)');
    }

    if (typeof options === 'function') {
      cb = options;
      options = {};
    }

    if (typeof data === 'number') data = data.toString();

    if (this.readyState !== WebSocket.OPEN) {
      sendAfterClose(this, data, cb);
      return;
    }

    const opts = {
      binary: typeof data !== 'string',
      mask: !this._isServer,
      compress: true,
      fin: true,
      ...options
    };

    if (!this._extensions[PerMessageDeflate.extensionName]) {
      opts.compress = false;
    }

    this._sender.send(data || EMPTY_BUFFER, opts, cb);
  }

  /**
   * Forcibly close the connection.
   *
   * @public
   */
  terminate() {
    if (this.readyState === WebSocket.CLOSED) return;
    if (this.readyState === WebSocket.CONNECTING) {
      const msg = 'WebSocket was closed before the connection was established';
      abortHandshake(this, this._req, msg);
      return;
    }

    if (this._socket) {
      this._readyState = WebSocket.CLOSING;
      this._socket.destroy();
    }
  }
}

/**
 * @constant {Number} CONNECTING
 * @memberof WebSocket
 */
Object.defineProperty(WebSocket, 'CONNECTING', {
  enumerable: true,
  value: readyStates.indexOf('CONNECTING')
});

/**
 * @constant {Number} CONNECTING
 * @memberof WebSocket.prototype
 */
Object.defineProperty(WebSocket.prototype, 'CONNECTING', {
  enumerable: true,
  value: readyStates.indexOf('CONNECTING')
});

/**
 * @constant {Number} OPEN
 * @memberof WebSocket
 */
Object.defineProperty(WebSocket, 'OPEN', {
  enumerable: true,
  value: readyStates.indexOf('OPEN')
});

/**
 * @constant {Number} OPEN
 * @memberof WebSocket.prototype
 */
Object.defineProperty(WebSocket.prototype, 'OPEN', {
  enumerable: true,
  value: readyStates.indexOf('OPEN')
});

/**
 * @constant {Number} CLOSING
 * @memberof WebSocket
 */
Object.defineProperty(WebSocket, 'CLOSING', {
  enumerable: true,
  value: readyStates.indexOf('CLOSING')
});

/**
 * @constant {Number} CLOSING
 * @memberof WebSocket.prototype
 */
Object.defineProperty(WebSocket.prototype, 'CLOSING', {
  enumerable: true,
  value: readyStates.indexOf('CLOSING')
});

/**
 * @constant {Number} CLOSED
 * @memberof WebSocket
 */
Object.defineProperty(WebSocket, 'CLOSED', {
  enumerable: true,
  value: readyStates.indexOf('CLOSED')
});

/**
 * @constant {Number} CLOSED
 * @memberof WebSocket.prototype
 */
Object.defineProperty(WebSocket.prototype, 'CLOSED', {
  enumerable: true,
  value: readyStates.indexOf('CLOSED')
});

[
  'binaryType',
  'bufferedAmount',
  'extensions',
  'isPaused',
  'protocol',
  'readyState',
  'url'
].forEach((property) => {
  Object.defineProperty(WebSocket.prototype, property, { enumerable: true });
});

//
// Add the `onopen`, `onerror`, `onclose`, and `onmessage` attributes.
// See https://html.spec.whatwg.org/multipage/comms.html#the-websocket-interface
//
['open', 'error', 'close', 'message'].forEach((method) => {
  Object.defineProperty(WebSocket.prototype, `on${method}`, {
    enumerable: true,
    get() {
      for (const listener of this.listeners(method)) {
        if (listener[kForOnEventAttribute]) return listener[kListener];
      }

      return null;
    },
    set(handler) {
      for (const listener of this.listeners(method)) {
        if (listener[kForOnEventAttribute]) {
          this.removeListener(method, listener);
          break;
        }
      }

      if (typeof handler !== 'function') return;

      this.addEventListener(method, handler, {
        [kForOnEventAttribute]: true
      });
    }
  });
});

WebSocket.prototype.addEventListener = addEventListener;
WebSocket.prototype.removeEventListener = removeEventListener;

module.exports = WebSocket;

/**
 * Initialize a WebSocket client.
 *
 * @param {WebSocket} websocket The client to initialize
 * @param {(String|URL)} address The URL to which to connect
 * @param {Array} protocols The subprotocols
 * @param {Object} [options] Connection options
 * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether any
 *     of the `'message'`, `'ping'`, and `'pong'` events can be emitted multiple
 *     times in the same tick
 * @param {Boolean} [options.autoPong=true] Specifies whether or not to
 *     automatically send a pong in response to a ping
 * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to wait
 *     for the closing handshake to finish after `websocket.close()` is called
 * @param {Function} [options.finishRequest] A function which can be used to
 *     customize the headers of each http request before it is sent
 * @param {Boolean} [options.followRedirects=false] Whether or not to follow
 *     redirects
 * @param {Function} [options.generateMask] The function used to generate the
 *     masking key
 * @param {Number} [options.handshakeTimeout] Timeout in milliseconds for the
 *     handshake request
 * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
 *     buffered data chunks
 * @param {Number} [options.maxFragments=16384] The maximum number of message
 *     fragments
 * @param {Number} [options.maxPayload=104857600] The maximum allowed message
 *     size
 * @param {Number} [options.maxRedirects=10] The maximum number of redirects
 *     allowed
 * @param {String} [options.origin] Value of the `Origin` or
 *     `Sec-WebSocket-Origin` header
 * @param {(Boolean|Object)} [options.perMessageDeflate=true] Enable/disable
 *     permessage-deflate
 * @param {Number} [options.protocolVersion=13] Value of the
 *     `Sec-WebSocket-Version` header
 * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
 *     not to skip UTF-8 validation for text and close messages
 * @private
 */
function initAsClient(websocket, address, protocols, options) {
  const opts = {
    allowSynchronousEvents: true,
    autoPong: true,
    closeTimeout: CLOSE_TIMEOUT,
    protocolVersion: protocolVersions[1],
    maxBufferedChunks: 256 * 1024,
    maxFragments: 16 * 1024,
    maxPayload: 100 * 1024 * 1024,
    skipUTF8Validation: false,
    perMessageDeflate: true,
    followRedirects: false,
    maxRedirects: 10,
    ...options,
    socketPath: undefined,
    hostname: undefined,
    protocol: undefined,
    timeout: undefined,
    method: 'GET',
    host: undefined,
    path: undefined,
    port: undefined
  };

  websocket._autoPong = opts.autoPong;
  websocket._closeTimeout = opts.closeTimeout;

  if (!protocolVersions.includes(opts.protocolVersion)) {
    throw new RangeError(
      `Unsupported protocol version: ${opts.protocolVersion} ` +
        `(supported versions: ${protocolVersions.join(', ')})`
    );
  }

  let parsedUrl;

  if (address instanceof URL) {
    parsedUrl = address;
  } else {
    try {
      parsedUrl = new URL(address);
    } catch {
      throw new SyntaxError(`Invalid URL: ${address}`);
    }
  }

  if (parsedUrl.protocol === 'http:') {
    parsedUrl.protocol = 'ws:';
  } else if (parsedUrl.protocol === 'https:') {
    parsedUrl.protocol = 'wss:';
  }

  websocket._url = parsedUrl.href;

  const isSecure = parsedUrl.protocol === 'wss:';
  const isIpcUrl = parsedUrl.protocol === 'ws+unix:';
  let invalidUrlMessage;

  if (parsedUrl.protocol !== 'ws:' && !isSecure && !isIpcUrl) {
    invalidUrlMessage =
      'The URL\'s protocol must be one of "ws:", "wss:", ' +
      '"http:", "https:", or "ws+unix:"';
  } else if (isIpcUrl && !parsedUrl.pathname) {
    invalidUrlMessage = "The URL's pathname is empty";
  } else if (parsedUrl.hash) {
    invalidUrlMessage = 'The URL contains a fragment identifier';
  }

  if (invalidUrlMessage) {
    const err = new SyntaxError(invalidUrlMessage);

    if (websocket._redirects === 0) {
      throw err;
    } else {
      emitErrorAndClose(websocket, err);
      return;
    }
  }

  const defaultPort = isSecure ? 443 : 80;
  const key = randomBytes(16).toString('base64');
  const request = isSecure ? https.request : http.request;
  const protocolSet = new Set();
  let perMessageDeflate;

  opts.createConnection =
    opts.createConnection || (isSecure ? tlsConnect : netConnect);
  opts.defaultPort = opts.defaultPort || defaultPort;
  opts.port = parsedUrl.port || defaultPort;
  opts.host = parsedUrl.hostname.startsWith('[')
    ? parsedUrl.hostname.slice(1, -1)
    : parsedUrl.hostname;
  opts.headers = {
    ...opts.headers,
    'Sec-WebSocket-Version': opts.protocolVersion,
    'Sec-WebSocket-Key': key,
    Connection: 'Upgrade',
    Upgrade: 'websocket'
  };
  opts.path = parsedUrl.pathname + parsedUrl.search;
  opts.timeout = opts.handshakeTimeout;

  if (opts.perMessageDeflate) {
    perMessageDeflate = new PerMessageDeflate({
      ...opts.perMessageDeflate,
      isServer: false,
      maxPayload: opts.maxPayload
    });
    opts.headers['Sec-WebSocket-Extensions'] = format({
      [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
    });
  }
  if (protocols.length) {
    for (const protocol of protocols) {
      if (
        typeof protocol !== 'string' ||
        !subprotocolRegex.test(protocol) ||
        protocolSet.has(protocol)
      ) {
        throw new SyntaxError(
          'An invalid or duplicated subprotocol was specified'
        );
      }

      protocolSet.add(protocol);
    }

    opts.headers['Sec-WebSocket-Protocol'] = protocols.join(',');
  }
  if (opts.origin) {
    if (opts.protocolVersion < 13) {
      opts.headers['Sec-WebSocket-Origin'] = opts.origin;
    } else {
      opts.headers.Origin = opts.origin;
    }
  }
  if (parsedUrl.username || parsedUrl.password) {
    opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
  }

  if (isIpcUrl) {
    const parts = opts.path.split(':');

    opts.socketPath = parts[0];
    opts.path = parts[1];
  }

  let req;

  if (opts.followRedirects) {
    if (websocket._redirects === 0) {
      websocket._originalIpc = isIpcUrl;
      websocket._originalSecure = isSecure;
      websocket._originalHostOrSocketPath = isIpcUrl
        ? opts.socketPath
        : parsedUrl.host;

      const headers = options && options.headers;

      //
      // Shallow copy the user provided options so that headers can be changed
      // without mutating the original object.
      //
      options = { ...options, headers: {} };

      if (headers) {
        for (const [key, value] of Object.entries(headers)) {
          options.headers[key.toLowerCase()] = value;
        }
      }
    } else if (websocket.listenerCount('redirect') === 0) {
      const isSameHost = isIpcUrl
        ? websocket._originalIpc
          ? opts.socketPath === websocket._originalHostOrSocketPath
          : false
        : websocket._originalIpc
          ? false
          : parsedUrl.host === websocket._originalHostOrSocketPath;

      if (!isSameHost || (websocket._originalSecure && !isSecure)) {
        //
        // Match curl 7.77.0 behavior and drop the following headers. These
        // headers are also dropped when following a redirect to a subdomain.
        //
        delete opts.headers.authorization;
        delete opts.headers.cookie;

        if (!isSameHost) delete opts.headers.host;

        opts.auth = undefined;
      }
    }

    //
    // Match curl 7.77.0 behavior and make the first `Authorization` header win.
    // If the `Authorization` header is set, then there is nothing to do as it
    // will take precedence.
    //
    if (opts.auth && !options.headers.authorization) {
      options.headers.authorization =
        'Basic ' + Buffer.from(opts.auth).toString('base64');
    }

    req = websocket._req = request(opts);

    if (websocket._redirects) {
      //
      // Unlike what is done for the `'upgrade'` event, no early exit is
      // triggered here if the user calls `websocket.close()` or
      // `websocket.terminate()` from a listener of the `'redirect'` event. This
      // is because the user can also call `request.destroy()` with an error
      // before calling `websocket.close()` or `websocket.terminate()` and this
      // would result in an error being emitted on the `request` object with no
      // `'error'` event listeners attached.
      //
      websocket.emit('redirect', websocket.url, req);
    }
  } else {
    req = websocket._req = request(opts);
  }

  if (opts.timeout) {
    req.on('timeout', () => {
      abortHandshake(websocket, req, 'Opening handshake has timed out');
    });
  }

  req.on('error', (err) => {
    if (req === null || req[kAborted]) return;

    req = websocket._req = null;
    emitErrorAndClose(websocket, err);
  });

  req.on('response', (res) => {
    const location = res.headers.location;
    const statusCode = res.statusCode;

    if (
      location &&
      opts.followRedirects &&
      statusCode >= 300 &&
      statusCode < 400
    ) {
      if (++websocket._redirects > opts.maxRedirects) {
        abortHandshake(websocket, req, 'Maximum redirects exceeded');
        return;
      }

      req.abort();

      let addr;

      try {
        addr = new URL(location, address);
      } catch (e) {
        const err = new SyntaxError(`Invalid URL: ${location}`);
        emitErrorAndClose(websocket, err);
        return;
      }

      initAsClient(websocket, addr, protocols, options);
    } else if (!websocket.emit('unexpected-response', req, res)) {
      abortHandshake(
        websocket,
        req,
        `Unexpected server response: ${res.statusCode}`
      );
    }
  });

  req.on('upgrade', (res, socket, head) => {
    websocket.emit('upgrade', res);

    //
    // The user may have closed the connection from a listener of the
    // `'upgrade'` event.
    //
    if (websocket.readyState !== WebSocket.CONNECTING) return;

    req = websocket._req = null;

    const upgrade = res.headers.upgrade;

    if (upgrade === undefined || upgrade.toLowerCase() !== 'websocket') {
      abortHandshake(websocket, socket, 'Invalid Upgrade header');
      return;
    }

    const digest = createHash('sha1')
      .update(key + GUID)
      .digest('base64');

    if (res.headers['sec-websocket-accept'] !== digest) {
      abortHandshake(websocket, socket, 'Invalid Sec-WebSocket-Accept header');
      return;
    }

    const serverProt = res.headers['sec-websocket-protocol'];
    let protError;

    if (serverProt !== undefined) {
      if (!protocolSet.size) {
        protError = 'Server sent a subprotocol but none was requested';
      } else if (!protocolSet.has(serverProt)) {
        protError = 'Server sent an invalid subprotocol';
      }
    } else if (protocolSet.size) {
      protError = 'Server sent no subprotocol';
    }

    if (protError) {
      abortHandshake(websocket, socket, protError);
      return;
    }

    if (serverProt) websocket._protocol = serverProt;

    const secWebSocketExtensions = res.headers['sec-websocket-extensions'];

    if (secWebSocketExtensions !== undefined) {
      if (!perMessageDeflate) {
        const message =
          'Server sent a Sec-WebSocket-Extensions header but no extension ' +
          'was requested';
        abortHandshake(websocket, socket, message);
        return;
      }

      let extensions;

      try {
        extensions = parse(secWebSocketExtensions);
      } catch (err) {
        const message = 'Invalid Sec-WebSocket-Extensions header';
        abortHandshake(websocket, socket, message);
        return;
      }

      const extensionNames = Object.keys(extensions);

      if (
        extensionNames.length !== 1 ||
        extensionNames[0] !== PerMessageDeflate.extensionName
      ) {
        const message = 'Server indicated an extension that was not requested';
        abortHandshake(websocket, socket, message);
        return;
      }

      try {
        perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
      } catch (err) {
        const message = 'Invalid Sec-WebSocket-Extensions header';
        abortHandshake(websocket, socket, message);
        return;
      }

      websocket._extensions[PerMessageDeflate.extensionName] =
        perMessageDeflate;
    }

    websocket.setSocket(socket, head, {
      allowSynchronousEvents: opts.allowSynchronousEvents,
      generateMask: opts.generateMask,
      maxBufferedChunks: opts.maxBufferedChunks,
      maxFragments: opts.maxFragments,
      maxPayload: opts.maxPayload,
      skipUTF8Validation: opts.skipUTF8Validation
    });
  });

  if (opts.finishRequest) {
    opts.finishRequest(req, websocket);
  } else {
    req.end();
  }
}

/**
 * Emit the `'error'` and `'close'` events.
 *
 * @param {WebSocket} websocket The WebSocket instance
 * @param {Error} The error to emit
 * @private
 */
function emitErrorAndClose(websocket, err) {
  websocket._readyState = WebSocket.CLOSING;
  //
  // The following assignment is practically useless and is done only for
  // consistency.
  //
  websocket._errorEmitted = true;
  websocket.emit('error', err);
  websocket.emitClose();
}

/**
 * Create a `net.Socket` and initiate a connection.
 *
 * @param {Object} options Connection options
 * @return {net.Socket} The newly created socket used to start the connection
 * @private
 */
function netConnect(options) {
  options.path = options.socketPath;
  return net.connect(options);
}

/**
 * Create a `tls.TLSSocket` and initiate a connection.
 *
 * @param {Object} options Connection options
 * @return {tls.TLSSocket} The newly created socket used to start the connection
 * @private
 */
function tlsConnect(options) {
  options.path = undefined;

  if (!options.servername && options.servername !== '') {
    options.servername = net.isIP(options.host) ? '' : options.host;
  }

  return tls.connect(options);
}

/**
 * Abort the handshake and emit an error.
 *
 * @param {WebSocket} websocket The WebSocket instance
 * @param {(http.ClientRequest|net.Socket|tls.Socket)} stream The request to
 *     abort or the socket to destroy
 * @param {String} message The error message
 * @private
 */
function abortHandshake(websocket, stream, message) {
  websocket._readyState = WebSocket.CLOSING;

  const err = new Error(message);
  Error.captureStackTrace(err, abortHandshake);

  if (stream.setHeader) {
    stream[kAborted] = true;
    stream.abort();

    if (stream.socket && !stream.socket.destroyed) {
      //
      // On Node.js >= 14.3.0 `request.abort()` does not destroy the socket if
      // called after the request completed. See
      // https://github.com/websockets/ws/issues/1869.
      //
      stream.socket.destroy();
    }

    process.nextTick(emitErrorAndClose, websocket, err);
  } else {
    stream.destroy(err);
    stream.once('error', websocket.emit.bind(websocket, 'error'));
    stream.once('close', websocket.emitClose.bind(websocket));
  }
}

/**
 * Handle cases where the `ping()`, `pong()`, or `send()` methods are called
 * when the `readyState` attribute is `CLOSING` or `CLOSED`.
 *
 * @param {WebSocket} websocket The WebSocket instance
 * @param {*} [data] The data to send
 * @param {Function} [cb] Callback
 * @private
 */
function sendAfterClose(websocket, data, cb) {
  if (data) {
    const length = isBlob(data) ? data.size : toBuffer(data).length;

    //
    // The `_bufferedAmount` property is used only when the peer is a client and
    // the opening handshake fails. Under these circumstances, in fact, the
    // `setSocket()` method is not called, so the `_socket` and `_sender`
    // properties are set to `null`.
    //
    if (websocket._socket) websocket._sender._bufferedBytes += length;
    else websocket._bufferedAmount += length;
  }

  if (cb) {
    const err = new Error(
      `WebSocket is not open: readyState ${websocket.readyState} ` +
        `(${readyStates[websocket.readyState]})`
    );
    process.nextTick(cb, err);
  }
}

/**
 * The listener of the `Receiver` `'conclude'` event.
 *
 * @param {Number} code The status code
 * @param {Buffer} reason The reason for closing
 * @private
 */
function receiverOnConclude(code, reason) {
  const websocket = this[kWebSocket];

  websocket._closeFrameReceived = true;
  websocket._closeMessage = reason;
  websocket._closeCode = code;

  if (websocket._socket[kWebSocket] === undefined) return;

  websocket._socket.removeListener('data', socketOnData);
  process.nextTick(resume, websocket._socket);

  if (code === 1005) websocket.close();
  else websocket.close(code, reason);
}

/**
 * The listener of the `Receiver` `'drain'` event.
 *
 * @private
 */
function receiverOnDrain() {
  const websocket = this[kWebSocket];

  if (!websocket.isPaused) websocket._socket.resume();
}

/**
 * The listener of the `Receiver` `'error'` event.
 *
 * @param {(RangeError|Error)} err The emitted error
 * @private
 */
function receiverOnError(err) {
  const websocket = this[kWebSocket];

  if (websocket._socket[kWebSocket] !== undefined) {
    websocket._socket.removeListener('data', socketOnData);

    //
    // On Node.js < 14.0.0 the `'error'` event is emitted synchronously. See
    // https://github.com/websockets/ws/issues/1940.
    //
    process.nextTick(resume, websocket._socket);

    websocket.close(err[kStatusCode]);
  }

  if (!websocket._errorEmitted) {
    websocket._errorEmitted = true;
    websocket.emit('error', err);
  }
}

/**
 * The listener of the `Receiver` `'finish'` event.
 *
 * @private
 */
function receiverOnFinish() {
  this[kWebSocket].emitClose();
}

/**
 * The listener of the `Receiver` `'message'` event.
 *
 * @param {Buffer|ArrayBuffer|Buffer[])} data The message
 * @param {Boolean} isBinary Specifies whether the message is binary or not
 * @private
 */
function receiverOnMessage(data, isBinary) {
  this[kWebSocket].emit('message', data, isBinary);
}

/**
 * The listener of the `Receiver` `'ping'` event.
 *
 * @param {Buffer} data The data included in the ping frame
 * @private
 */
function receiverOnPing(data) {
  const websocket = this[kWebSocket];

  if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
  websocket.emit('ping', data);
}

/**
 * The listener of the `Receiver` `'pong'` event.
 *
 * @param {Buffer} data The data included in the pong frame
 * @private
 */
function receiverOnPong(data) {
  this[kWebSocket].emit('pong', data);
}

/**
 * Resume a readable stream
 *
 * @param {Readable} stream The readable stream
 * @private
 */
function resume(stream) {
  stream.resume();
}

/**
 * The `Sender` error event handler.
 *
 * @param {Error} The error
 * @private
 */
function senderOnError(err) {
  const websocket = this[kWebSocket];

  if (websocket.readyState === WebSocket.CLOSED) return;
  if (websocket.readyState === WebSocket.OPEN) {
    websocket._readyState = WebSocket.CLOSING;
    setCloseTimer(websocket);
  }

  //
  // `socket.end()` is used instead of `socket.destroy()` to allow the other
  // peer to finish sending queued data. There is no need to set a timer here
  // because `CLOSING` means that it is already set or not needed.
  //
  this._socket.end();

  if (!websocket._errorEmitted) {
    websocket._errorEmitted = true;
    websocket.emit('error', err);
  }
}

/**
 * Set a timer to destroy the underlying raw socket of a WebSocket.
 *
 * @param {WebSocket} websocket The WebSocket instance
 * @private
 */
function setCloseTimer(websocket) {
  websocket._closeTimer = setTimeout(
    websocket._socket.destroy.bind(websocket._socket),
    websocket._closeTimeout
  );
}

/**
 * The listener of the socket `'close'` event.
 *
 * @private
 */
function socketOnClose() {
  const websocket = this[kWebSocket];

  this.removeListener('close', socketOnClose);
  this.removeListener('data', socketOnData);
  this.removeListener('end', socketOnEnd);

  websocket._readyState = WebSocket.CLOSING;

  //
  // The close frame might not have been received or the `'end'` event emitted,
  // for example, if the socket was destroyed due to an error. Ensure that the
  // `receiver` stream is closed after writing any remaining buffered data to
  // it. If the readable side of the socket is in flowing mode then there is no
  // buffered data as everything has been already written. If instead, the
  // socket is paused, any possible buffered data will be read as a single
  // chunk.
  //
  if (
    !this._readableState.endEmitted &&
    !websocket._closeFrameReceived &&
    !websocket._receiver._writableState.errorEmitted &&
    this._readableState.length !== 0
  ) {
    const chunk = this.read(this._readableState.length);

    websocket._receiver.write(chunk);
  }

  websocket._receiver.end();

  this[kWebSocket] = undefined;

  clearTimeout(websocket._closeTimer);

  if (
    websocket._receiver._writableState.finished ||
    websocket._receiver._writableState.errorEmitted
  ) {
    websocket.emitClose();
  } else {
    websocket._receiver.on('error', receiverOnFinish);
    websocket._receiver.on('finish', receiverOnFinish);
  }
}

/**
 * The listener of the socket `'data'` event.
 *
 * @param {Buffer} chunk A chunk of data
 * @private
 */
function socketOnData(chunk) {
  if (!this[kWebSocket]._receiver.write(chunk)) {
    this.pause();
  }
}

/**
 * The listener of the socket `'end'` event.
 *
 * @private
 */
function socketOnEnd() {
  const websocket = this[kWebSocket];

  websocket._readyState = WebSocket.CLOSING;
  websocket._receiver.end();
  this.end();
}

/**
 * The listener of the socket `'error'` event.
 *
 * @private
 */
function socketOnError() {
  const websocket = this[kWebSocket];

  this.removeListener('error', socketOnError);
  this.on('error', NOOP);

  if (websocket) {
    websocket._readyState = WebSocket.CLOSING;
    this.destroy();
  }
}


/***/ },

/***/ 1259
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   kb: () => (/* reexport default from dynamic */ _lib_websocket_js__WEBPACK_IMPORTED_MODULE_6___default.a)
/* harmony export */ });
/* unused harmony import specifier */ var WebSocket;
/* harmony import */ var _lib_stream_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(3719);
/* harmony import */ var _lib_stream_js__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_lib_stream_js__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var _lib_extension_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(5926);
/* harmony import */ var _lib_permessage_deflate_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2971);
/* harmony import */ var _lib_permessage_deflate_js__WEBPACK_IMPORTED_MODULE_2___default = /*#__PURE__*/__webpack_require__.n(_lib_permessage_deflate_js__WEBPACK_IMPORTED_MODULE_2__);
/* harmony import */ var _lib_receiver_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(6286);
/* harmony import */ var _lib_receiver_js__WEBPACK_IMPORTED_MODULE_3___default = /*#__PURE__*/__webpack_require__.n(_lib_receiver_js__WEBPACK_IMPORTED_MODULE_3__);
/* harmony import */ var _lib_sender_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(914);
/* harmony import */ var _lib_sender_js__WEBPACK_IMPORTED_MODULE_4___default = /*#__PURE__*/__webpack_require__.n(_lib_sender_js__WEBPACK_IMPORTED_MODULE_4__);
/* harmony import */ var _lib_subprotocol_js__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(8237);
/* harmony import */ var _lib_websocket_js__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(1060);
/* harmony import */ var _lib_websocket_js__WEBPACK_IMPORTED_MODULE_6___default = /*#__PURE__*/__webpack_require__.n(_lib_websocket_js__WEBPACK_IMPORTED_MODULE_6__);
/* harmony import */ var _lib_websocket_server_js__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(1722);
/* harmony import */ var _lib_websocket_server_js__WEBPACK_IMPORTED_MODULE_7___default = /*#__PURE__*/__webpack_require__.n(_lib_websocket_server_js__WEBPACK_IMPORTED_MODULE_7__);











/* unused harmony default export */ var __WEBPACK_DEFAULT_EXPORT__ = ((/* unused pure expression or super */ null && (WebSocket)));


/***/ },

/***/ 3782
(module, __unused_webpack_exports, __webpack_require__) {

const x = (y) => {
	const x = {}; __webpack_require__.d(x, y); return x
} 
const y = (x) => (() => (x))
module.exports = x({ ["default"]: () => (__WEBPACK_EXTERNAL_MODULE_http__["default"]) });

/***/ },

/***/ 3871
(module, __unused_webpack_exports, __webpack_require__) {

const x = (y) => {
	const x = {}; __webpack_require__.d(x, y); return x
} 
const y = (x) => (() => (x))
module.exports = x({ ["default"]: () => (__WEBPACK_EXTERNAL_MODULE_https__["default"]) });

/***/ },

/***/ 181
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("buffer");

/***/ },

/***/ 6982
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("crypto");

/***/ },

/***/ 4434
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("events");

/***/ },

/***/ 8611
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("http");

/***/ },

/***/ 5692
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("https");

/***/ },

/***/ 9278
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("net");

/***/ },

/***/ 2203
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("stream");

/***/ },

/***/ 4756
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("tls");

/***/ },

/***/ 7016
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("url");

/***/ },

/***/ 9023
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("util");

/***/ },

/***/ 3106
(module) {

module.exports = __WEBPACK_EXTERNAL_createRequire_require("zlib");

/***/ }

/******/ });
/************************************************************************/
/******/ // The module cache
/******/ const __webpack_module_cache__ = {};
/******/ 
/******/ // The require function
/******/ function __webpack_require__(moduleId) {
/******/ 	// Check if module is in cache
/******/ 	const cachedModule = __webpack_module_cache__[moduleId];
/******/ 	if (cachedModule !== undefined) {
/******/ 		return cachedModule.exports;
/******/ 	}
/******/ 	// Create a new module (and put it into the cache)
/******/ 	const module = __webpack_module_cache__[moduleId] = {
/******/ 		// no module.id needed
/******/ 		// no module.loaded needed
/******/ 		exports: {}
/******/ 	};
/******/ 
/******/ 	// Execute the module function
/******/ 	__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 
/******/ 	// Return the exports of the module
/******/ 	return module.exports;
/******/ }
/******/ 
/************************************************************************/
/******/ /* webpack/runtime/compat get default export */
/******/ // getDefaultExport function for compatibility with non-harmony modules
/******/ __webpack_require__.n = (module) => {
/******/ 	const getter = module && module.__esModule ?
/******/ 		() => (module['default']) :
/******/ 		() => (module);
/******/ 	__webpack_require__.d(getter, { a: getter });
/******/ 	return getter;
/******/ };
/******/ 
/******/ /* webpack/runtime/define property getters */
/******/ // define getter/value functions for harmony exports
/******/ __webpack_require__.d = (exports, definition) => {
/******/ 	if(Array.isArray(definition)) {
/******/ 		var i = 0;
/******/ 		while(i < definition.length) {
/******/ 			var key = definition[i++];
/******/ 			var binding = definition[i++];
/******/ 			var descriptor = binding === 0 ? { enumerable: true, value: definition[i++] } : { enumerable: true, get: binding };
/******/ 			if(!__webpack_require__.o(exports, key)) Object.defineProperty(exports, key, descriptor);
/******/ 		}
/******/ 	} else {
/******/ 		for(var key in definition) {
/******/ 			if(__webpack_require__.o(definition, key) && !__webpack_require__.o(exports, key)) {
/******/ 				Object.defineProperty(exports, key, { enumerable: true, get: definition[key] });
/******/ 			}
/******/ 		}
/******/ 	}
/******/ };
/******/ 
/******/ /* webpack/runtime/hasOwnProperty shorthand */
/******/ __webpack_require__.o = (obj, prop) => (Object.prototype.hasOwnProperty.call(obj, prop));
/******/ 
/************************************************************************/
let __webpack_exports__ = {};
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   t: () => (/* binding */ send)
/* harmony export */ });
/* harmony import */ var _database_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(9564);
/* harmony import */ var _array_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(1224);
/* harmony import */ var _object_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(2210);



async function send(data) {
  if (!data || !data.method) {
    throw new Error("Invalid request: missing method property.");
  }
  const [type, method] = data.method.split(".");
  if (type === "database") return await (0,_database_js__WEBPACK_IMPORTED_MODULE_0__/* ["default"] */ .A)(method, data);
  if (type === "array") return await (0,_array_js__WEBPACK_IMPORTED_MODULE_1__/* ["default"] */ .A)(method, data);
  if (type === "object") return await (0,_object_js__WEBPACK_IMPORTED_MODULE_2__/* ["default"] */ .A)(method, data);
  throw new Error(`Unsupported query target type: "${type}" in method "${data.method}"`);
}
/* harmony default export */ const src = ({ send });

/* harmony export */ __webpack_require__.d(__webpack_exports__, [
/* harmony export */   "A", 0, src
/* harmony export */ ]);

const __webpack_exports__default = __webpack_exports__.A;
export { __webpack_exports__default as default, send };
