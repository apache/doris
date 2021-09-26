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
 * /* eslint-disable global-require, import/no-extraneous-dependencies, @typescript-eslint/no-var-requires
 *
 * @format
 */

module.exports = {
    plugins: [
        require('autoprefixer')(),
        // 修复一些 flex 的 bug
        require('postcss-flexbugs-fixes'),
        // 支持一些现代浏览器 CSS 特性，支持 browserslist
        require('postcss-preset-env')({
            // 自动添加浏览器头
            autoprefixer: {
                // will add prefixes only for final and IE versions of specification
                flexbox: 'no-2009'
            },
            stage: 3
        }),
        // 根据 browserslist 自动导入部分 normalize.css 的内容
        require('postcss-normalize')
    ]
};
