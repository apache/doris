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

import org.junit.Assert;

suite("test_root_user", "account") {
    String suiteName = "test_root_user"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'

    try_sql("DROP USER ${user}")
     sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """GRANT ADMIN_PRIV ON *.*.* TO ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
         test {
              sql """
                  alter user root identified by '123456';
              """
              exception "root"
        }

        test {
              sql """
                  set password for 'root' = password('123456');
              """
              exception "root"
            }
    }

}

