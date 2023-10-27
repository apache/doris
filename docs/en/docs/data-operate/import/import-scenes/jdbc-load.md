---
{
    "title": "Synchronize Data Using Insert Method",
    "language": "en"
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

# Synchronize Data Using Insert Method

Users can use INSERT statement to import data through MySQL protocol.

The INSERT statement is used in a similar way to the INSERT statement used in databases such as MySQL. The INSERT statement supports the following two syntaxes:

```sql
* INSERT INTO table SELECT ...
* INSERT INTO table VALUES(...)
````

Here we only introduce the second way. For a detailed description of the INSERT command, see the [INSERT](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md) command documentation.

## Single write

Single write means that the user directly executes an INSERT command. An example is as follows:

```sql
INSERT INTO example_tbl (col1, col2, col3) VALUES (1000, "test", 3.25);
````

For Doris, an INSERT command is a complete import transaction.

Therefore, whether it is importing one piece of data or multiple pieces of data, we do not recommend using this method for data import in the production environment. The INSERT operation of high-frequency words will result in a large number of small files in the storage layer, which will seriously affect the system performance.

This method is only used for simple offline tests or low-frequency operations.

Or you can use the following methods for batch insert operations:

```sql
INSERT INTO example_tbl VALUES
(1000, "baidu1", 3.25),
(2000, "baidu2", 4.25),
(3000, "baidu3", 5.25);
````

We recommend that the number of inserts in a batch be as large as possible, such as thousands or even 10,000 at a time. Or you can use PreparedStatement to perform batch inserts through the following procedure.

## JDBC example

Here we give a simple JDBC batch INSERT code example:

````java
package demo.doris;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DorisJDBCDemo {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true";
    private static final String HOST = "127.0.0.1"; // Leader Node host
    private static final int PORT = 9030; // query port of Leader Node
    private static final String DB = "demo";
    private static final String TBL = "test_1";
    private static final String USER = "admin";
    private static final String PASSWD = "my_pass";

    private static final int INSERT_BATCH_SIZE = 10000;

    public static void main(String[] args) {
        insert();
    }

    private static void insert() {
        // Be careful not to add a semicolon ";" at the end
        String query = "insert into " + TBL + " values(?, ?)";
        // Set Label to be idempotent.
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
````

Please note the following:

1. The JDBC connection string needs to add the `rewriteBatchedStatements=true` parameter and use the `PreparedStatement` method.

   Currently, Doris does not support PrepareStatemnt on the server side, so the JDBC Driver will perform batch Prepare on the client side.

   `rewriteBatchedStatements=true` will ensure that the Driver executes batches. And finally form an INSERT statement of the following form and send it to Doris:

   ```sql
   INSERT INTO example_tbl VALUES
   (1000, "baidu1", 3.25)
   (2000, "baidu2", 4.25)
   (3000, "baidu3", 5.25);
   ````

2. Batch size

   Because batch processing is performed on the client, if a batch is too large, it will occupy the memory resources of the client, so you need to pay attention.

   Doris will support PrepareStatemnt on the server in the future, so stay tuned.

3. Import atomicity

   Like other import methods, the INSERT operation itself supports atomicity. Each INSERT operation is an import transaction, which guarantees atomic writing of all data in an INSERT.

   As mentioned earlier, we recommend that when using INSERT to import data, use the "batch" method to import, rather than a single insert.

   At the same time, we can set a Label for each INSERT operation. Through the [Label mechanism](./load-atomicity.md), the idempotency and atomicity of operations can be guaranteed, and the data will not be lost or heavy in the end. For the specific usage of Label in INSERT, you can refer to the [INSERT](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md) document.
