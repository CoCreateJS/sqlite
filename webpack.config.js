import webpack from "webpack";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { EsbuildPlugin } from "esbuild-loader";
import { FileUploader } from "@cocreate/webpack";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default async (env, argv) => {
    const isProduction = argv && argv.mode === "production";

    const config = {
        // Target Node.js environment for Worker Threads execution
        target: "node",

        // Output native ES Module bundle syntax
        experiments: {
            outputModule: true
        },

        // Entry point for the SQLite / libSQL database adapter
        entry: {
            "sqlite": "./src/index.js"
        },

        output: {
            path: path.resolve(__dirname, "dist"),
            // Produces 'sqlite.bundle.js'
            filename: "[name].bundle.js",
            library: {
                type: "module"
            },
            clean: true,
            // Retain standard global scope in worker threads
            globalObject: "this"
        },

        resolve: {
            alias: {
                // Route any @libsql/client imports directly to the lightweight WebSocket transport driver,
                // bypassing heavy C/C++ native binaries (@libsql/sqlite3, native drivers) entirely.
                "@libsql/client$": "@libsql/client/ws"
            }
        },

        node: {
            __dirname: false,
            __filename: false
        },

        plugins: [
            // Ignore optional native C/C++ bindings, binary modules, and local database drivers
            // to prevent Webpack resolution warnings and keep bundle size lightweight.
            new webpack.IgnorePlugin({
                resourceRegExp: /^(@libsql\/sqlite3|better-sqlite3|@libsql\/client-sqlite3|@libsql\/client\/node|.*\.node)$/
            }),
            // Uploads bundled driver script to CDN/HTTP endpoint if configured
            new FileUploader(env, argv)
        ],

        mode: isProduction ? "production" : "development",
        devtool: false,

        module: {
            rules: [
                {
                    test: /\.js$/,
                    exclude: /node_modules/,
                    use: {
                        loader: "esbuild-loader",
                        options: {
                            loader: "js",
                            target: "node18"
                        }
                    }
                }
            ]
        },

        optimization: {
            minimize: isProduction,
            minimizer: [
                new EsbuildPlugin({
                    target: "node18"
                })
            ],
            // Enforce single-file output by disabling chunk splitting
            splitChunks: false,
            runtimeChunk: false
        },

        performance: {
            hints: isProduction ? "warning" : false
        }
    };

    return config;
};