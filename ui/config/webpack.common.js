/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
const path = require('path');
const paths = require('./paths');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const {CleanWebpackPlugin} = require('clean-webpack-plugin');
const transformerFactory = require('ts-import-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const postCssLoader = () => {
    return {
        loader: 'postcss-loader',
        options: {
            plugins: [
                require('autoprefixer'),
                require('cssnano')
            ]
        }
    };
};
const devMode = process.env.NODE_ENV === 'dev';
module.exports = {
    // 入口文件
    entry: paths.entryApp,
    output: {
        path: paths.distSrc,
        // publicPath: '',
        filename: '[name].[hash].js',
        chunkFilename: '[name].[hash].js'
    },
    devtool: 'source-map',
    resolve: {
        extensions: ['*', '.ts', '.tsx', 'jsx', '.js', 'json'],
        alias: {
            Components: paths.Components,
            Src: paths.Src,
            Utils: paths.Utils,
            Models: paths.Models,
            Services: paths.Services,
            Constants: paths.Constants,
            '@hooks': paths['@hooks']
        }
    },
    module: {
        rules: [
            // {
            //     test: /\.ext$/,
            //     use: ['cache-loader'],
            //     include: path.resolve('src')
            // },
            {
                test: /\.(ts|tsx)$/,
                exclude: /node_modules/,
                include: paths.srcPath,
                use: [
                    'cache-loader',
                    {
                        loader: 'ts-loader',
                        options: {
                            // disable type checker
                            transpileOnly: true,
                            // ts import plugin
                            getCustomTransformers: () => ({
                                before: [
                                    transformerFactory({style: true}),
                                    transformerFactory({
                                        style: false,
                                        libraryName: 'lodash',
                                        camel2DashComponentName: false
                                    })
                                ]
                            }),
                            compilerOptions: {
                                module: 'es2015'
                            }
                        }
                    },
                    {
                        loader: 'thread-loader',
                        options: {
                            workers: 2
                        }
                    }
                ]

            },
            {
                test: /\.(png|jpg|gif|ttf|eot|svg|woff|woff2)$/,
                loader: 'url-loader',
                options: {
                    limit: 1000
                }
            },
            {
                test: /\.css$/,
                use: [
                    devMode ? {loader: 'style-loader'} : MiniCssExtractPlugin.loader,
                    {loader: 'css-loader'},
                    postCssLoader(),
                    {
                        loader: 'thread-loader',
                        options: {
                            workers: 2
                        }
                    }
                ]
            },
            // For pure CSS (without CSS modules)
            {
                test: /\.less?$/,
                include: /node_modules/,
                use: [
                    devMode ? {loader: 'style-loader'} : MiniCssExtractPlugin.loader,
                    {
                        loader: 'css-loader'
                    },
                    postCssLoader(),
                    {
                        loader: 'less-loader',
                        options: {
                            // modifyVars: {...themes},
                            javascriptEnabled: true
                        }
                    },
                    {
                        loader: 'thread-loader',
                        options: {
                            workers: 2
                        }
                    }
                ]
            },
            // For CSS modules
            {
                test: /\.less?$/,
                exclude: /node_modules/,
                use: [
                    devMode ? {loader: 'style-loader'} : MiniCssExtractPlugin.loader,
                    {
                        loader: 'css-loader',
                        options: {
                            modules: true
                        }
                    },
                    postCssLoader(),
                    {
                        loader: 'less-loader',
                        options: {
                            // modifyVars: {...themes},
                            javascriptEnabled: true
                        }
                    },
                    {
                        loader: 'thread-loader',
                        options: {
                            workers: 2
                        }
                    }
                ]
            }
        ]
    },
    optimization: {
        splitChunks: {
            cacheGroups: {
                commons: {
                    name: 'commons',
                    test: /[\\/]node_modules[\\/]/,
                    chunks: 'async',
                    minSize: 300000,
                    maxSize: 500000,
                    minChunks: 2
                },
                lodash: {
                    name: 'lodash',
                    chunks: 'all',
                    test: /lodash/,
                    minChunks: 2,
                    priority: 10
                },
                moment: {
                    name: 'moment',
                    chunks: 'all',
                    test: /moment/,
                    minChunks: 2,
                    priority: 10
                },
                codeMirror: {
                    name: 'codemirror',
                    chunks: 'all',
                    test: /codemirror/,
                    minChunks: 2,
                    priority: 10
                },
                styles: {
                    name: 'styles',
                    test: /\.(le|c)ss$/,
                    chunks: 'all',
                    enforce: true,
                    priority: 100,
                    minSize: 0,
                    maxSize: 30000000
                }
            }
        }
    },
    // 配置相应的插件
    plugins: [
        new HtmlWebpackPlugin({
            template: './src/index.html',
            inject: 'body',
            favicon: './src/favicon.ico'
        }),
        new MiniCssExtractPlugin({
            filename: '[name].[hash].css',
            ignoreOrder: true
        }),
        new CleanWebpackPlugin()
    ]
};
