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

package org.apache.doris.nereids.rules.rewrite;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PrepareTest {
    @Test
    public void prepare() {
        // 数据库连接信息
        // String url = "jdbc:mysql://127.0.0.1:9030/maldb?useServerPrepStmts=true";
        String url = "jdbc:mysql://127.0.0.1:9030/maldb?rewriteBatchedStatements=true&defaultFetchSize=500&useCursorFetch=true";
        String username = "root";
        String password = "";

        // String sql = "with cte1 as ( select cast ( date_trunc ( col_date_undef_signed_not_null , ? ) as datetime ) col_alias43212 , min_by ( col_datetime_6__undef_signed_not_null , pk ) col_alias43213 from table_200_undef_partitions2_keys3_properties4_distributed_by55 group by col_alias43212 order by col_alias43212 , col_alias43213 ) , cte2 as ( select col_alias43213 from cte1 where col_alias43213 < ? ) select * from cte2";
        // String sql = "insert into t4 values (?, cast(? as varchar(3)), ?)";
        // String sql = "insert into t4 values (?, ?, ?)";
        // String sql = "select v1 from t4 where `key`=?";
        // String sql = " select min ( pk - ? ) pk , pk as pk from table_20_undef_partitions2_keys3_properties4_distributed_by54 tbl_alias1 group by pk having ( pk >= pk ) or ( round ( sign ( sign ( pk ) ) ) - ? < ? ) order by pk ";
        // String sql = "select (? + col_largeint_undef_signed_not_null)  col_alias27416 , col_largeint_undef_signed  AS col_alias27417 , min_by (col_largeint_undef_signed_not_null, pk)  AS col_alias27418  from table_200_undef_partitions2_keys3_properties4_distributed_by53   GROUP BY col_alias27416,col_alias27417 having col_alias27416 = col_alias27418  ORDER BY col_alias27416,col_alias27417,col_alias27418 ;";
        // String sql = "select least(col_largeint_undef_signed, ?, col_smallint_undef_signed_not_null, col_smallint_undef_signed_not_null)  col_alias39343   from table_200_undef_partitions2_keys3_properties4_distributed_by54";
        // String sql = "select cast ( date_trunc ( col_datetime_undef_signed_not_null , ? ) as datetime ) col_alias5871 from table_200_undef_partitions2_keys3_properties4_distributed_by52 order by col_alias5871";
        // String sql = "select col_struct_c_char_255_char_255___c_char_100_char_100___c_varchar_255_varchar_255___c_varchar_65533_varchar_65533___c_string_string__undef_signed_not_null , max ( col_char_255__undef_signed ) over ( order by pk rows between ? preceding and current row ) as col_alias27416 from table_20_undef_partitions2_keys3_properties4_distributed_by56 where elt ( ? , col_char_100__undef_signed , struct_element ( col_struct_c_char_255_char_255___c_char_100_char_100___c_varchar_255_varchar_255___c_varchar_65533_varchar_65533___c_string_string__undef_signed , ? ) , col_varchar_65533__undef_signed ) = ?";
        // String sql = "SELECT IFNULL(col_date, ?) FROM t3 where pk = ?";
        // String sql = "select array_apply([1,2,3], ?, ?)";
        String sql = "select ? from t4";
        try {
            // 显式加载 MySQL JDBC 驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 或者对于旧版本的 MySQL 驱动：
            // Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            PreparedStatement stmt = connection.prepareStatement(sql);

            // stmt.setInt(1, 2);
            // stmt.setString(2, "aabsssssswerwerwerwwers");
            // stmt.setString(3, "2");
            // stmt.execute();
            // stmt.setString(1, "3");
            // stmt.setString(2, "aabcgaaa");
            // stmt.setString(3, "3");
            // stmt.execute();

            // stmt.setBigDecimal(1, BigDecimal.valueOf(100.2));
            stmt.setString(1, "=");
            // stmt.setInt(2, 1);
            // stmt.setInt(2, 0);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                // 根据列索引获取数据
                System.out.println(resultSet.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
