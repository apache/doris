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

/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
const devConfig = require('./config/webpack.dev');
const prodConfig = require('./config/webpack.prod');

const SpeedMeasurePlugin = require('speed-measure-webpack-plugin');

const smp = new SpeedMeasurePlugin();

let config = devConfig;

switch (process.env.NODE_ENV) {
    case 'prod':
    case 'production':
        config = prodConfig;
        break;
    case 'dev':
    case 'development':
        config = devConfig;
        break;
    default:
        config = devConfig;
}

const webpackConfig = config;

module.exports = smp.wrap(webpackConfig);
