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
const helpers = require('./helpers');

module.exports = {
    entryApp: helpers.root('/src/index.tsx'),
    entryHTML: helpers.root('/src/index.ejs'),
    entryJSP: helpers.root('/src/index.jsp'),
    distSrc: helpers.root('/dist'),
    staticPath: helpers.root('/public'),
    Components: helpers.root('/src/components'),
    Src: helpers.root('/src'),
    Utils: helpers.root('/src/utils'),
    Models: helpers.root('/src/models'),
    Services: helpers.root('/src/services'),
    Constants: helpers.root('/src/constants'),
    '@hooks': helpers.root('/src/hooks'),
    '@src': helpers.root('/src'),
};
