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
const merge = require('webpack-merge');
const baseConfig = require('./webpack.common.js');
const path = require('path');

module.exports = merge(baseConfig, {
    // 设置为开发模式
    mode: 'development',
    devtool: 'inline-source-map',
    optimization: {
        minimize: false
    },
    // 配置服务端目录和端口
    devServer: {
        historyApiFallback: true,
        disableHostCheck: true,
        stats: 'minimal',
        compress: true,
        overlay: true,
        hot: false,
        host: 'localhost',
        open: true,
        contentBase: path.join(__dirname, 'dist'),
        port: 8030,
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:8030',
                changeOrigin: true,
                secure: false
                // pathRewrite: {'^/commonApi': '/commonApi'}
            },
            '/rest': {
                target: 'http://127.0.0.1:8030',
                changeOrigin: true,
                secure: false
                // pathRewrite: {'^/commonApi': '/commonApi'}
            }
        }
    }
});
