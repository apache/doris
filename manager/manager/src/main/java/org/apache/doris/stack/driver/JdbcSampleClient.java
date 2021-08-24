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

package org.apache.doris.stack.driver;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
@Slf4j
public class JdbcSampleClient {

    @Autowired
    public JdbcSampleClient() {
    }

    public boolean testConnetion(String host, int port, String dbName, String user,
                              String passwd) throws SQLException {
        StringBuffer buffer = new StringBuffer();
        buffer.append("jdbc:mysql://");
        buffer.append(host);
        buffer.append(":");
        buffer.append(port);
        buffer.append("/");
        buffer.append(dbName);
        String url = buffer.toString();
        try {
            Connection myCon = DriverManager.getConnection(url, user, passwd);
            myCon.close();
            return true;
        } catch (SQLException e) {
            log.error("Get JDBC connection exception.");
            throw e;
        }
    }
}
