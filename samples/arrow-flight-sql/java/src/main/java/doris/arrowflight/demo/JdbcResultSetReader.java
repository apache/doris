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

package doris.arrowflight.demo;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Iterate over each row in jdbc ResultSet.
 */
public class JdbcResultSetReader {

    @FunctionalInterface
    public interface LoadJdbcResultSetFunc {
        void load(ResultSet resultSet) throws IOException, SQLException;
    }

    public static LoadJdbcResultSetFunc loadJdbcResult = resultSet -> {
        int rowCount = 0;
        final int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            rowCount += 1;
        }
        System.out.println("> rowCount: " + rowCount + ", columnCount: " + columnCount);
    };

    public static LoadJdbcResultSetFunc loadJdbcResultToString = resultSet -> {
        int rowCount = 0;
        final int columnCount = resultSet.getMetaData().getColumnCount();
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            StringBuilder line = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                line.append(resultSet.getString(i)).append(",");
            }
            if (rowCount == 0) { // only print first line
                System.out.println("> " + line);
            }
            rowCount += 1;
            result.add(line.toString());
        }
        System.out.println(
                "> rowCount: " + rowCount + ", columnCount: " + columnCount + " resultSize: " + result.size());
    };
}
