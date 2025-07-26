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

package org.apache.doris.regression.util

import com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin

/**
 * Custom authentication plugin for Doris JDBC that extends MysqlClearPasswordPlugin
 * but doesn't require SSL for clear password authentication.
 *
 * This is useful when connecting to Doris FE which doesn't enforce SSL for clear text password.
 */
class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {

    /**
     * Overrides the parent method to indicate that SSL is not required
     * for clear password authentication.
     *
     * @return false to indicate SSL is not required
     */
    @Override
    boolean requiresConfidentiality() {
        return false
    }
}
