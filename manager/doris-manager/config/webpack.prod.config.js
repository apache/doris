// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

const webpackBaseConfig = require("./webpack.base.config");
const { merge } = require("webpack-merge");
const path = require("path");
const TerserWebpackPlugin = require('terser-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
// const { BundleAnalyzerPlugin } = require('webpack-bundle-analyzer')
// const CopyPlugin = require('copy-webpack-plugin');

module.exports = merge(webpackBaseConfig, {
    mode: 'production',
    output: {
        publicPath: '/',
    },
    optimization: {
        minimizer: [
            new TerserWebpackPlugin({
                terserOptions: {
                    compress: {
                        drop_console: true,
                    },
                    sourceMap: true,
                },
                parallel: true,
            }),
        ],
        chunkIds: 'named',
        moduleIds: 'deterministic',
        splitChunks: {
            name: false,
            chunks: 'all',
            minChunks: 1,
            // maxAsyncRequests: 5,
            // maxInitialRequests: 5,
            cacheGroups: {
                vendor: {
                    test: /node_modules/,
                    name: 'vendor',
                    chunks: 'all',
                    enforce: true,
                },
                antd: {
                    test: /antd?/,
                    name: 'antd',
                    priority: 10,
                    chunks: 'initial',
                    enforce: true,
                },
                react: {
                    test: /react|react-dom|mobx|prop-type/,
                    name: 'react',
                    priority: 10,
                    chunks: 'initial',
                    enforce: true,
                },
            },
        },
        runtimeChunk: {
            name: entrypoint => `runtime-${entrypoint.name}`,
        },
    },
    plugins: [
        new CleanWebpackPlugin(),
        new MiniCssExtractPlugin({
            filename: 'assets/css/[name].[contenthash].css',
            chunkFilename: 'assets/css/[name].[contenthash].css',
        }),
        // new CopyPlugin({
        //     patterns: [{ from: path.join(__dirname, '../src/assets/'), to: path.join(__dirname, '../dist/src/assets/') }],
        // }),
        // new BundleAnalyzerPlugin()
    ],
});
