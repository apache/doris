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

suite("test_mysql_connection") {

    String cmdDefault = "mysql -uroot -h172.19.0.2 -P9131 -e \"show variables\"";
    String cmdDisabledSsl= "mysql --ssl-mode=DISABLE -uroot -h172.19.0.2 -P9131 -e \"show variables\"";
    String cmdSsl12 = "mysql --ssl-mode=REQUIRED -uroot -h172.19.0.2 -P9131 --tls-version=TLSv1.2 -e \"show variables\"";
    // The current mysql-client version of the test environment is 5.7.32, which does not support TLSv1.3, so comment this part.
    // String cmdSsl13 = "mysql --ssl-mode=REQUIRED -uroot -h172.19.0.2 -P9131 --tls-version=TLSv1.3 -e \"show variables\"";
    executeMySQLCommand(cmdDefault);
    executeMySQLCommand(cmdDisabledSsl);
    executeMySQLCommand(cmdSsl12);
    // executeMySQLCommand(cmdSsl13);
}
