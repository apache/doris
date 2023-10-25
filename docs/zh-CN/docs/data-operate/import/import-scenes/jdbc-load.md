---
{
    "title": "使用 Insert 方式同步数据",
    "language": "zh-CN"
}

---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# 使用 Insert 方式同步数据

用户可以通过 MySQL 协议，使用 INSERT 语句进行数据导入。

INSERT 语句的使用方式和 MySQL 等数据库中 INSERT 语句的使用方式类似。 INSERT 语句支持以下两种语法：

```sql
* INSERT INTO table SELECT ...
* INSERT INTO table VALUES(...)
```

这里我们仅介绍第二种方式。关于 INSERT 命令的详细说明，请参阅 [INSERT](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md) 命令文档。

## 单次写入

单次写入是指用户直接执行一个 INSERT 命令。示例如下：

```sql
INSERT INTO example_tbl (col1, col2, col3) VALUES (1000, "test", 3.25);
```

对于 Doris 来说，一个 INSERT 命令就是一个完整的导入事务。

因此不论是导入一条数据，还是多条数据，我们都不建议在生产环境使用这种方式进行数据导入。高频次的 INSERT 操作会导致在存储层产生大量的小文件，会严重影响系统性能。

该方式仅用于线下简单测试或低频少量的操作。

或者可以使用以下方式进行批量的插入操作：

```sql
INSERT INTO example_tbl VALUES
(1000, "baidu1", 3.25),
(2000, "baidu2", 4.25),
(3000, "baidu3", 5.25);
```

我们建议一批次插入条数在尽量大，比如几千甚至一万条一次。或者可以通过下面的程序的方式，使用 PreparedStatement 来进行批量插入。

## JDBC 示例

这里我们给出一个简单的 JDBC 批量 INSERT 代码示例：

```java
package demo.doris;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DorisJDBCDemo {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true";
    private static final String HOST = "127.0.0.1"; // Leader Node host
    private static final int PORT = 9030;   // query_port of Leader Node
    private static final String DB = "demo";
    private static final String TBL = "test_1";
    private static final String USER = "admin";
    private static final String PASSWD = "my_pass";

    private static final int INSERT_BATCH_SIZE = 10000;

    public static void main(String[] args) {
        insert();
    }

    private static void insert() {
        // 注意末尾不要加 分号 ";"
        String query = "insert into " + TBL + " values(?, ?)";
        // 设置 Label 以做到幂等。
        // String query = "insert into " + TBL + " WITH LABEL my_label values(?, ?)";

        Connection conn = null;
        PreparedStatement stmt = null;
        String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT, DB);
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(dbUrl, USER, PASSWD);
            stmt = conn.prepareStatement(query);

            for (int i =0; i < INSERT_BATCH_SIZE; i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, i * 100);
                stmt.addBatch();
            }

            int[] res = stmt.executeBatch();
            System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
```

请注意以下几点：

1. JDBC 连接串需添加 `rewriteBatchedStatements=true` 参数，并使用 `PreparedStatement` 方式。

   目前 Doris 暂不支持服务器端的 PrepareStatemnt，所以 JDBC Driver 会在客户端进行批量 Prepare。

   `rewriteBatchedStatements=true` 会确保 Driver 执行批处理。并最终形成如下形式的 INSERT 语句发往 Doris：

   ```sql
   INSERT INTO example_tbl VALUES
   (1000, "baidu1", 3.25)
   (2000, "baidu2", 4.25)
   (3000, "baidu3", 5.25);
   ```

2. 批次大小

   因为是在客户端进行批量处理，所以一批次过大的话，会占用客户端的内存资源，需关注。

   Doris 后续会支持服务端的 PrepareStatemnt，敬请期待。

3. 导入原子性

   和其他到导入方式一样，INSERT 操作本身也支持原子性。每一个 INSERT 操作都是一个导入事务，能够保证一个 INSERT 中的所有数据原子性的写入。

   前面提到，我们建议在使用 INSERT 导入数据时，采用 ”批“ 的方式进行导入，而不是单条插入。

   同时，我们可以为每次 INSERT 操作设置一个 Label。通过 [Label 机制](./load-atomicity.md) 可以保证操作的幂等性和原子性，最终做到数据的不丢不重。关于 INSERT 中 Label 的具体用法，可以参阅 [INSERT](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md) 文档。
