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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_bootstrap_authentication_test_mode", "docker,account") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 1
    options.beNum = 1
    options.feConfigs.add('require_initial_root_password=true')
    docker(options) {
        connect('root', '', cluster.jdbcUrl) {
            qt_empty_root 'SELECT 1'
        }
        connect('admin', '', cluster.jdbcUrl) {
            qt_empty_admin 'SELECT 1'
        }
    }
}
