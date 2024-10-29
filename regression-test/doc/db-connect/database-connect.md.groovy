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

import org.junit.jupiter.api.Assertions
import java.sql.*
suite("docs/db-connect/database-connect.md") {
    String user = "root";
    String password = "";
    String newUrl = "jdbc:mysql://127.0.0.1:9030/test?useUnicode=true&characterEncoding=utf8&useTimezone=true&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true";
    try {
        Connection myCon = DriverManager.getConnection(newUrl, user, password);
        Statement stmt = myCon.createStatement();
        ResultSet result = stmt.executeQuery("show databases");
        ResultSetMetaData metaData = result.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (result.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(result.getObject(i));
            }
        }
    } catch (SQLException e) {
        log.error("get JDBC connection exception.", e)
        Assertions.fail("examples in docs/db-connect/database-connect.md failed to exec, please fix it", t)
    }
}
