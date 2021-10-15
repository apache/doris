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

const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const devMode = process.env.NODE_ENV === 'dev' || process.env.NODE_ENV === 'development';
const themes = require('../theme')();

const postCssLoader = () => {
    return {
        loader: 'postcss-loader',
        options: {
            postcssOptions: {
                plugins: [require('autoprefixer'), require('cssnano')],
            },
        },
    };
};

module.exports = {
    entry: {
        index: ['react-hot-loader/patch'].concat([path.resolve(__dirname, '../src/index.tsx')]),
    },
    output: {
        path: path.join(__dirname, '../dist/'),
        publicPath: '/',
        filename: 'assets/js/[name].[contenthash:8].js',
        chunkFilename: 'assets/js/[name].[contenthash:8].js',
        sourceMapFilename: 'assets/js/[name].[contenthash:8].js.map',
    },
    resolve: {
        alias: {
            '@src': path.resolve(__dirname, '../src'),
            '@assets': path.resolve(__dirname, '../src/assets'),
            '@components': path.resolve(__dirname, '../src/components'),
            '@models': path.resolve(__dirname, '../src/models'),
            '@router': path.resolve(__dirname, '../src/router'),
            '@pages': path.resolve(__dirname, '../src/pages'),
            '@utils': path.resolve(__dirname, '../src/utils'),
            '@tools': path.resolve(__dirname, '../src/tools'),
        },
        extensions: ['.ts', '.tsx', '.js', '.jsx'],
    },
    module: {
        rules: [
            {
                test: /\.ts[x]?$/,
                exclude: /node_modules/,
                use: {
                    loader: 'babel-loader',
                    options: {
                        presets: [['@babel/preset-env', { targets: 'defaults' }]],
                    },
                },
            },
            {
                test: /\.css$/,
                use: [
                    {
                        loader: 'style-loader',
                    },
                    {
                        loader: 'css-loader',
                        options: {
                            sourceMap: true,
                        },
                    },
                    {
                        loader: 'postcss-loader',
                        options: {
                            sourceMap: true,
                        },
                    },
                ],
            },
            // For pure CSS (without CSS modules)
            {
                test: /\.less?$/,
                include: /node_modules/,
                use: [
                    devMode ? { loader: 'style-loader' } : MiniCssExtractPlugin.loader,
                    {
                        loader: 'css-loader',
                    },
                    postCssLoader(),
                    {
                        loader: 'less-loader',
                        options: {
                            lessOptions: {
                                modifyVars: {...themes},
                                javascriptEnabled: true,
                            },
                            implementation: require('less'),
                        },
                    },
                ],
            },
            // For CSS modules
            {
                test: /\.less?$/,
                exclude: /node_modules/,
                use: [
                    devMode ? { loader: 'style-loader' } : MiniCssExtractPlugin.loader,
                    {
                        loader: 'css-loader',
                        options: {
                            modules: true,
                        },
                    },
                    postCssLoader(),
                    {
                        loader: 'less-loader',
                        options: {
                            lessOptions: {
                                modifyVars: {...themes},
                                javascriptEnabled: true,
                            },
                            implementation: require('less'),
                        },
                    },
                ],
            },
            {
                test: /\.(svg|png|jpg|gif|ttf|eot|otf|svg|woff(2)?)(\?[a-z0-9]+)?$/,
                loader: 'url-loader',
                options: {
                    limit: 2 * 1024,
                    name: '[path][name].[ext]',
                },
            },
        ],
    },
    plugins: [
        new HtmlWebpackPlugin({
            filename: 'index.html',
            template: 'public/index.html',
            inject: 'body',
            minify: false,
        }),
        new MiniCssExtractPlugin({
            filename: '[name].[hash].css',
            ignoreOrder: true,
        }),
    ],
};
