/********************************************************************************
 * Copyright (C) 2026 CoCreate and Contributors.
 *
 * Licensed under GNU AGPLv3.
 ********************************************************************************/

import path from "node:path";
import { fileURLToPath } from "node:url";
import webpack from "webpack";
import { FileUploader } from "@cocreate/webpack";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default async (env, argv) => {
    const isProduction = argv && argv.mode === "production";

    // Base configuration shared across all build targets
    const baseConfig = {
        target: "node",
        experiments: {
            outputModule: true
        },
        resolve: {
            alias: {
                // Force pure WebSocket Hrana transport to bypass native C++ binaries (sqlite3, better-sqlite3)
                "@libsql/client$": "@libsql/client/ws"
            },
            extensions: [".js", ".mjs", ".cjs", ".json", ".ts"],
            fullySpecified: false
        },
        node: {
            __dirname: false,
            __filename: false
        },
        // Externalize native binary drivers to guarantee zero C++ compilation failures
        externals: [
            function ({ request }, callback) {
                if (/\.node$/i.test(request) || /^(sqlite3|better-sqlite3|@libsql\/sqlite3)$/i.test(request)) {
                    return callback(null, `commonjs ${request}`);
                }
                callback();
            }
        ],
        devtool: false,
        module: {
            rules: [
                {
                    test: /\.m?[jc]s$/,
                    type: "javascript/auto",
                    resolve: {
                        fullySpecified: false
                    }
                },
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
                },
                {
                    test: /\.ts$/,
                    exclude: [/node_modules/, /\.d\.ts$/],
                    use: {
                        loader: "esbuild-loader",
                        options: {
                            loader: "ts",
                            target: "node18"
                        }
                    }
                }
            ]
        },
        performance: {
            hints: false
        }
    };

    const commonPlugins = [
        new webpack.optimize.LimitChunkCountPlugin({
            maxChunks: 1
        }),
        new webpack.IgnorePlugin({
            resourceRegExp: /^(cardinal|gcp-metadata|aws-sdk|bufferutil|utf-8-validate|\.d\.ts)$/
        })
    ];

    const distributionConfig = {
        ...baseConfig,
        name: "distribution",
        mode: isProduction ? "production" : "development",
        entry: "./src/index.js",
        output: {
            path: path.resolve(__dirname, "dist"),
            filename: "sqlite.js",
            library: {
                type: "module"
            },
            clean: true,
            globalObject: "this"
        },
        optimization: {
            // Keep driver and utility identifiers intact in production.
            minimize: false,
            concatenateModules: false,
            splitChunks: false,
            runtimeChunk: false
        },
        plugins: [
            ...commonPlugins,
            new FileUploader(env, argv)
        ]
    };

    return [distributionConfig];
};
