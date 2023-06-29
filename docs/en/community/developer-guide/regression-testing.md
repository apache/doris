---
{
    "title": "Regression Testing",
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

# Regression Testing

## Concepts
1. `Suite`: A test case, currently used to refer to the test case filename.
2. `Group`: A set of test cases, currently used to refer to the directory that contains the test cases.
3. `Action`: A pre-defined test action, such as the `sql` Action for executing SQL queries, the `test` Action for result validation, the `streamLoad` Action for data import, etc.

## Testing Steps
1. Make sure the cluster is installed properly.
2. Modify the configuration file `${DORIS_HOME}/regression-test/conf/regression-conf.groovy` and set the JDBC URL, user, and other configuration options.
3. Create a test case file and implement the test cases.
4. If the test case file contains a `qt` Action, create the associated data file(s). For example, in the case of `suites/demo/qt_action.groovy`, you need the `data/demo/qt_action.out` TSV file to validate the output.
5. Run `${DORIS_HOME}/run-regression-test.sh` to test all the cases, or run `${DORIS_HOME}/run-regression-test.sh --run <suiteName>` to test specific cases. For more examples, see the section "Examples of Running Startup Scripts".

## Directory Structure
Important files/directories to pay attention to during development:
1. `run-regression-test.sh`: The startup script.
2. `regression-conf.groovy`: Default configuration for regression testing.
3. `data`: Directory for storing input and output verification data.
4. `suites`: Directory for storing test cases.

```
./${DORIS_HOME}
    |-- **run-regression-test.sh**           // Startup script
    |-- regression-test
    |   |-- plugins                          // Plugin directory
    |   |-- conf
    |   |   |-- logback.xml                  // Logging config file
    |   |   |-- **regression-conf.groovy**   // General config file
    |   |
    |   |-- framework                        // Source code for testing framework
    |   |-- **data**                     // In/out files for test
    |   |   |-- demo                     // In/out files for demo
    |   |   |-- correctness              // In/out files for correctness test
    |   |   |-- performance              // In/out files for performance test
    |   |   |-- utils                    // In/out files for utils
    |   |
    |   |-- **suites**                   // Regression test cases
    |       |-- demo                     // Demo for test cases
    |       |-- correctness              // Demo for correctness test cases
    |       |-- performance              // Demo for performance test cases
    |       |-- utils                    // Other utils
    |
    |-- output
        |-- regression-test
            |-- log                          // Regression test logs
```



## Default Framework Configuration
During testing, the JDBC and FE configurations need to be modified according to the actual situation.

```groovy
/* ==== Only need to focus on the following part in most cases ==== */
// Default database. If not created, it will attempt to create this db
defaultDb = "regression_test"

// JDBC configuration
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?"
jdbcUser = "root"
jdbcPassword = ""

// FE address configuration, used for stream load
feHttpAddress = "127.0.0.1:8030"
feHttpUser = "root"
feHttpPassword = ""

/* ======== Generally, no need to modify the following part ======== */

// DORIS_HOME variable is passed in through run-regression-test.sh
// e.g., java -DDORIS_HOME=./

// Set the directory for regression test cases
suitePath = "${DORIS_HOME}/regression-test/suites"
// Set the directory for input/output data
dataPath = "${DORIS_HOME}/regression-test/data"
// Set the directory for plugins
pluginPath = "${DORIS_HOME}/regression-test/plugins"

// By default, all groups are loaded. Multiple groups can be separated by commas, e.g., "demo,performance"
// Generally, no need to modify here in the config file. Instead, use run-regression-test.sh --run -g to specify and override dynamically
testGroups = ""
// All suites are loaded by default. You can also specify and override dynamically using run-regression-test.sh --run -s
testSuites = ""
// Directories where test cases are loaded by default. You can also specify and override dynamically using run-regression-test.sh --run -d
testDirectories = ""

// Exclude test cases from these groups. You can specify and override dynamically using run-regression-test.sh --run -xg
excludeGroups = ""
// Exclude these test suites. You can specify and override dynamically using run-regression-test.sh --run -xs
excludeSuites = ""
// Exclude these directories. You can specify and override dynamically using run-regression-test.sh --run -xd
excludeDirectories = ""

// Other custom configurations
customConf1 = "test_custom_conf_value"
```

## Writing Test Cases
1. Go to the `${DORIS_HOME}/regression-test` directory.
2. Choose the appropriate directory based on the purpose of the test. The `suites/correctness` directory is for correctness testing, while the `suites/performance` directory is for performance testing.
3. Create a new Groovy test case file and add multiple `Action`s for testing. Actions will be explained in detail in the following sections.

## Action
An `Action` is a pre-defined test behavior provided by the testing framework, defined using DSL (Domain-Specific Language).

### sql action
The sql action is used to execute SQL queries and retrieve results. It throws an exception if the query fails.

Parameters:
- `String sql`: The input SQL query string.
- `return List<List<Object>>`: The query result. If it is a DDL/DML query, it returns one row and one column, with the unique value being the `updateRowCount`.

Here is an example code located at `${DORIS_HOME}/regression-test/suites/demo/sql_action.groovy`:

```groovy
suite("sql_action", "demo") {
    // execute sql and ignore result
    sql "show databases"

    // execute sql and get result, outer List denote rows, inner List denote columns in a single row
    List<List<Object>> tables = sql "show tables"

    // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
    assertTrue(tables.size() >= 0) // test rowCount >= 0

    // syntax error
    try {
        sql "a b c d e"
        throw new IllegalStateException("Should be syntax error")
    } catch (java.sql.SQLException t) {
        assertTrue(true)
    }

    def testTable = "test_sql_action1"

    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                            id int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

        // DDL/DML return 1 row and 1 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        def result2 = sql "INSERT INTO test_sql_action1 values(1), (2), (3)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 3, "Insert should update 3 rows")
    } finally {
        /**
         * try_xxx(args) means:
         *
         * try {
         *    return xxx(args)
         * } catch (Throwable t) {
         *     // do nothing
         *     return null
         * }
         */
        try_sql("DROP TABLE IF EXISTS ${testTable}")

        // you can see the error sql will not throw exception and return
        try {
            def errorSqlResult = try_sql("a b c d e f g")
            assertTrue(errorSqlResult == null)
        } catch (Throwable t) {
            assertTrue(false, "Never catch exception")
        }
    }

    // order_sql(sqlStr) equals to sql(sqlStr, isOrder=true)
    // sort result by string dict
    def list = order_sql """
                select 2
                union all
                select 1
                union all
                select null
                union all
                select 15
                union all
                select 3
                """

    assertEquals(null, list[0][0])
    assertEquals(1, list[1][0])
    assertEquals(15, list[2][0])
    assertEquals(2, list[3][0])
    assertEquals(3, list[4][0])
}
```

### qt action
The qt action is used to execute SQL queries and validate the results using the associated `.out` TSV file.

Parameters:
- `String sql`: The input SQL query string.
- `return void`

Here is an example code located at `${DORIS_HOME}/regression-test/suites/demo/qt_action.groovy`:

```groovy
suite("qt_action", "demo") {
    /**
     * qt_xxx sql equals to quickTest(xxx, sql) witch xxx is tag.
     * the result will be compare to the relate file: ${DORIS_HOME}/regression_test/data/qt_action.out.
     *
     * if you want to generate .out tsv file for real execute result. you can run with -genOut or -forceGenOut option.
     * e.g
     *   ${DORIS_HOME}/run-regression-test.sh --run qt_action -genOut
     *   ${DORIS_HOME}/run-regression-test.sh --run qt_action -forceGenOut
     */
    qt_select "select 1, 'beijing' union all select 2, 'shanghai'"

    qt_select2 "select 2"

    // order result by string dict then compare to .out file.
    // order_qt_xxx sql equals to quickTest(xxx, sql, true).
    order_qt_union_all  """
                select 2
                union all
                select 1
                union all
                select null
                union all
                select 15
                union all
                select 3
                """
}
```

### test action
The test action allows more complex validation rules for testing, such as validating row count, execution time, or whether an exception is thrown.

Available parameters:
- `String sql`: The input SQL query string.
- `List<List<Object>> result`: Provide a `List` object to compare the actual query result with the `List` object for equality.
- `Iterator<Object> resultIterator`: Provide an `Iterator` object to compare the actual query result with the `Iterator` for equality.
- `String resultFile`: Provide a file URI (can be a local file path or an HTTP(S) URL) to compare the actual query result with the contents of the file. The format should be similar to the `.out` file format, but without block headers and comments.
- `String exception`: Validate if the thrown exception contains certain strings.
- `long rowNum`: Validate the number of result rows.
- `long time`: Validate if the execution time is less than this value, in milliseconds.
- `Closure<List<List<Object>>, Throwable, Long, Long> check`: Custom callback validation. You can pass the result, exception, and time as arguments to the callback function. When a callback function is provided, other validation methods will be ignored.

Here is an example code located at `${DORIS_HOME}/regression-test/suites/demo/test_action.groovy`:

```groovy
suite("test_action", "demo") {
    test {
        sql "abcdefg"
        // check exception message contains
        exception "errCode = 2, detailMessage = Syntax error"
    }

    test {
        sql """
            select *
            from (
                select 1 id
                union all
                select 2
            ) a
            order by id"""

        // multi check condition

        // check return 2 rows
        rowNum 2
        // execute time must <= 5000 millisecond
        time 5000
        // check result, must be 2 rows and 1 column, the first row is 1, second is 2
        result(
            [[1], [2]]
        )
    }

    test {
        sql "a b c d e f g"

        // other check will not work because already declared a check callback
        exception "aaaaaaaaa"

        // callback
        check { result, exception, startTime, endTime ->
            // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
            assertTrue(exception != null)
        }
    }

    test {
        sql  """
                select 2
                union all
                select 1
                union all
                select null
                union all
                select 15
                union all
                select 3
                """

        check { result, ex, startTime, endTime ->
            // same as order_sql(sqlStr)
            result = sortRows(result)

            assertEquals(null, result[0][0])
            assertEquals(1, result[1][0])
            assertEquals(15, result[2][0])
            assertEquals(2, result[3][0])
            assertEquals(3, result[4][0])
        }
    }

    // execute sql and order query result, then compare to iterator
    def selectValues = [1, 2, 3, 4]
    test {
        order true
        sql selectUnionAll(selectValues)
        resultIterator(selectValues.iterator())
    }

    // compare to data/demo/test_action.csv
    test {
        order true
        sql selectUnionAll(selectValues)

        // you can set to http://xxx or https://xxx
        // and compare to http response body
        resultFile "test_action.csv"
    }
}
```



### explain action
The explain action is used to validate if the explain result contains certain strings.

Available parameters:
- `String sql`: The SQL query to be executed, excluding the "explain" keyword.
- `String contains`: Validates if the explain result contains specific strings. Multiple calls can be made to validate multiple results.
- `String notContains`: Validates if the explain result does not contain specific strings. Multiple calls can be made to validate multiple results.
- `Closure<String> check`: Custom validation callback function that can access the returned string. When a check function is provided, other validation methods are ignored.
- `Closure<String, Throwable, Long, Long> check`: Custom validation callback function that can additionally access the exception and execution time.

Here is an example code located at `${DORIS_HOME}/regression-test/suites/demo/explain_action.groovy`:

```groovy
suite("explain_action", "demo") {
    explain {
        sql("select 100")

        // contains("OUTPUT EXPRS:<slot 0> 100\n") && contains("PARTITION: UNPARTITIONED\n")
        contains "OUTPUT EXPRS:<slot 0> 100\n"
        contains "PARTITION: UNPARTITIONED\n"
    }

    explain {
        sql("select 100")

        // contains(" 100\n") && !contains("abcdefg") && !("1234567")
        contains " 100\n"
        notContains "abcdefg"
        notContains "1234567"
    }

    explain {
        sql("select 100")
        // simple callback
        check { explainStr -> explainStr.contains("abcdefg") || explainStr.contains(" 100\n") }
    }

    explain {
        sql("a b c d e")
        // callback with exception and time
        check { explainStr, exception, startTime, endTime ->
            // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
            assertTrue(exception != null)
        }
    }
}
```

### streamLoad action
The streamLoad action is used for data import.

Available parameters:
- String db: The database name. The default value is the `defaultDb` defined in `regression-conf.groovy`.
- String table: The table name.
- String file: The file path to import, which can be a relative path within the `data` directory or an HTTP URL to import a network file.
- `Iterator<List<Object>> inputIterator`: The iterator to import.
- String inputText: The text to import (less commonly used).
- InputStream inputStream: The byte stream to import (less commonly used).
- long time: Validates if the execution time is less than the specified value in milliseconds.
- void set(String key, String value): Sets the HTTP request headers for stream load, such as label, columnSeparator.
- `Closure<String, Throwable, Long, Long> check`: Custom validation callback function that can access the returned result, exception, and timeout. When a check function is provided, other validation items are ignored.

Here is an example code located at `${DORIS_HOME}/regression-test/suites/demo/streamLoad_action.groovy`:

```groovy
suite("streamLoad_action", "demo") {

    def tableName = "test_streamload_action1"

    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id int,
                name varchar(255)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            ) 
        """

    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'streamload_input.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
    }


    // stream load 100 rows
    def rowCount = 100
    // range: [0, rowCount)
    // or rangeClosed: [0, rowCount]
    def rowIt = range(0, rowCount)
            .mapToObj({i -> [i, "a_" + i]}) // change Long to List<Long, String>
            .iterator()

    streamLoad {
        table tableName
        // also, you can upload a memory iterator
        inputIterator rowIt

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }
}
```

### Other Actions
Other available Actions include `thread`, `lazyCheck`, `events`, `connect`, and `selectUnionAll`. You can find specific examples in the directory `${DORIS_HOME}/regression-test/suites/demo`.

## Examples of Running Startup Scripts

```shell
# View script parameter descriptions
./run-regression-test.sh h

# View framework parameter descriptions
./run-regression-test.sh --run -h

# Test all test cases
./run-regression-test.sh

# Delete compiled test framework and test logs
./run-regression-test.sh --clean

# Test the test case with suiteName "sql_action" (suiteName is currently the prefix of the file name, and in this example, it corresponds to the "sql_action.groovy" test case file)
./run-regression-test.sh --run sql_action

# Test test cases with suiteName containing 'sql' (note that you need to enclose it in single quotes)
./run-regression-test.sh --run '*sql*'

# Test "demo" and "performance" groups
./run-regression-test.sh --run -g 'demo,performance'

# Test "sql_action" under the "demo" group
./run-regression-test.sh --run -g demo -s sql_action

# Test "sql_action" under the "demo" directory
./run-regression-test.sh --run -d demo -s sql_action

# Test cases under the "demo" directory, excluding the "sql_action" test case
./run-regression-test.sh --run -d demo -xs sql_action

# Exclude test cases under the "demo" directory
./run-regression-test.sh --run -xd demo

# Exclude test cases under the "demo" group
./run-regression-test.sh --run -xg demo

# Custom configuration
./run-regression-test.sh --run -conf a=b

# Concurrent execution
./run-regression-test.sh --run -parallel 5 -suiteParallel 10 -actionParallel 20

```



## Automatically Generating .out Files from Query Results

```shell
# Automatically generate the .out file for the "sql_action" test case based on query results (ignores if the .out file already exists)
./run-regression-test.sh --run sql_action -genOut

# Automatically generate the .out file for the "sql_action" test case based on query results (overwrites the existing .out file if it exists)
./run-regression-test.sh --run sql_action -forceGenOut
```



## Suite Plugins
Sometimes we need to extend the `Suite` class but it's not convenient to modify the source code of the Suite class. In such cases, we can use plugins to extend the functionality. The default plugin directory is `${DORIS_HOME}/regression-test/plugins`, where you can define extension methods using Groovy scripts. Here's an example `plugin_example.groovy` that adds a `testPlugin` function to the Suite class for logging:

```groovy
import org.apache.doris.regression.suite.Suite

// register `testPlugin` function to Suite,
// and invoke in ${DORIS_HOME}/regression-test/suites/demo/test_plugin.groovy
Suite.metaClass.testPlugin = { String info /* param */ ->

    // which suite invoke current function?
    Suite suite = delegate as Suite

    // function body
    suite.getLogger().info("Test plugin: suiteName: ${suite.name}, info: ${info}".toString())

    // optional return value
    return "OK"
}

logger.info("Added 'testPlugin' function to Suite")
```

After adding the `testPlugin` function, you can use it in regular test cases. Here's an example located at `${DORIS_HOME}/regression-test/suites/demo/test_plugin.groovy`:

```groovy
suite("test_plugin", "demo") {
    // register testPlugin function in ${DORIS_HOME}/regression-test/plugins/plugin_example.groovy
    def result = testPlugin("message from suite")
    assertEquals("OK", result)
}
```

## CI/CD Support
### TeamCity
TeamCity can recognize Service Messages through stdout. When the regression testing framework is launched with the `--teamcity` parameter, the framework will print TeamCity Service Messages to stdout. TeamCity will automatically read the event log from stdout and display the "Tests" section in the current pipeline, showing the tests and their logs.
Therefore, you only need to configure the following command to launch the regression testing framework. The `-Dteamcity.enableStdErr=false` parameter allows error logs to be printed to stdout for chronological log analysis.

```shell
JAVA_OPTS="-Dteamcity.enableStdErr=${enableStdErr}" ./run-regression-test.sh --teamcity --run
```



## External Data Source E2E Testing

Doris supports querying some externally deployed data sources. Therefore, the regression framework also provides the functionality to set up external data sources using Docker Compose for end-to-end (E2E) testing of Doris with external data sources.

0. Prerequisites

    Before starting Docker, please modify the `CONTAINER_UID` variable in the `docker/thirdparties/custom_settings.env` file.

    You can modify it to something like: `doris-10002-18sda1-`.

    The startup script will replace the corresponding container names in the docker compose, ensuring that the container names and networks of multiple sets of the environment do not conflict.

1. Start Containers

   Doris currently supports Docker Compose for various data sources such as es, mysql, pg, hive, sqlserver, oracle, iceberg, hudi, trino, etc. The related files are located in the `docker/thirdparties/docker-compose` directory.

   By default, you can start all the Docker containers for external data sources with the following command:
   (Note: The hive and hudi containers require downloading pre-built data files, please refer to the respective hive and hudi documentation.)

   ```shell
   cd docker/thirdparties && sh run-thirdparties-docker.sh
   ```

   This command requires root or sudo privileges. If the command is successful, it means that all the containers have been started. You can use the `docker ps -a` command to check.

   To stop all the containers, you can use the following command:

   ```shell
   cd docker/thirdparties && sh run-thirdparties-docker.sh --stop
   ```

   You can also start or stop specific components using the following commands:

   ```shell
   cd docker/thirdparties
   # start mysql
   sh run-thirdparties-docker.sh -c mysql
   # start mysql,pg,iceberg
   sh run-thirdparties-docker.sh -c mysql,pg,iceberg
   # stop mysql,pg,iceberg
   sh run-thirdparties-docker.sh -c mysql,pg,iceberg --stop
   ```

   1. MySQL

      MySQL-related Docker Compose files are located in `docker/thirdparties/docker-compose/mysql`.

      * `mysql-5.7.yaml.tpl`: Docker Compose template file, no modifications needed. The default username and password are root/123456.
      * `mysql-5.7.env`: Configuration file where you can configure the port exposed by the MySQL container (default: 3316).
      * `init/`: This directory contains SQL files that will be automatically executed after the container is created. It currently creates databases, tables, and inserts a small amount of data.
      * `data/`: This is the local data directory mounted after the container starts. The `run-thirdparties-docker.sh` script automatically clears and rebuilds this directory each time it starts.

   2. PostgreSQL

      PostgreSQL-related Docker Compose files are located in `docker/thirdparties/docker-compose/postgresql`.

      * `postgresql-14.yaml.tpl`: Docker Compose template file, no modifications needed. The default username and password are postgres/123456.
      * `postgresql-14.env`: Configuration file where you can configure the port exposed by the PostgreSQL container (default: 5442).
      * `init/`: This directory contains SQL files that will be automatically executed after the container is created. It currently creates databases, tables, and inserts a small amount of data.
      * `data/`: This is the local data directory mounted after the container starts. The `run-thirdparties-docker.sh` script automatically clears and rebuilds this directory each time it starts.

   3. Hive

      Hive-related Docker compose files are located in `docker/thirdparties/docker-compose/hive` directory.

      - `hive-2x.yaml.tpl`: Docker compose file template, no need to modify.
      - `hadoop-hive.env.tpl`: Configuration file template, no need to modify.
      - `gen_env.sh`: Script to initialize configuration files. You can modify `FS_PORT` and `HMS_PORT`, which correspond to the defaultFs and Hive metastore ports. The default values are 8120 and 9183. The `run-thirdparties-docker.sh` script automatically calls this script during startup.
      - `scripts/` directory: Mounted to the container after startup. The files inside this directory do not need to be modified. However, before starting the container, you need to download the pre-built file: Download the `tpch1.db.tar.gz` file from `https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/tpch1_parquet/tpch1.db.tar.gz` and extract it into the `scripts/` directory.

   4. Elasticsearch

      Docker images for ES6, ES7, and ES8 versions are located in `docker/thirdparties/docker-compose/elasticsearch/` directory.

      - `es.yaml.tpl`: Docker compose file template for ES6, ES7, and ES8 versions. No need to modify.
      - `es.env`: Configuration file. You need to configure the ES port number.
      - `scripts/` directory: Contains initialization scripts for the launched images.

   5. Oracle

      Oracle 11 image is provided and located in `docker/thirdparties/docker-compose/oracle/` directory.

      - `oracle-11.yaml.tpl`: Docker compose file template. No need to modify.
      - `oracle-11.env`: Configuration for Oracle's external port. Default is 1521.

   6. SQLServer

      SQLServer 2022 image is provided and located in `docker/thirdparties/docker-compose/sqlserver/` directory.

      - `sqlserver.yaml.tpl`: Docker compose file template. No need to modify.
      - `sqlserver.env`: Configuration for SQLServer's external port. Default is 1433.

   7. ClickHouse

      ClickHouse 22 image is provided and located in `docker/thirdparties/docker-compose/clickhouse/` directory.

      - `clickhouse.yaml.tpl`: Docker compose file template. No need to modify.
      - `clickhouse.env`: Configuration for ClickHouse's external port. Default is 8123.

   8. Iceberg

      Iceberg + Spark + Minio image combination is provided and located in `docker/thirdparties/docker-compose/iceberg/` directory.

      - `iceberg.yaml.tpl`: Docker compose file template. No need to modify.
      - `entrypoint.sh.tpl`: Template for the initialization script after image startup. No need to modify.
      - `spark-defaults.conf.tpl`: Spark configuration file template. No need to modify.
      - `iceberg.env`: External port configuration file. You need to modify the external ports to avoid conflicts.

      After startup, you can start the Spark SQL using the following command: `docker exec -it doris-xx-spark-iceberg spark-sql` where `doris-xx-spark-iceberg` is the container name.

      Example of Spark SQL queries for Iceberg:

      ```
      create database db1;
      show databases;
      create table db1.test1(k1 bigint, k2 bigint, k3 string) partitioned by (k1);
      insert into db1.test1 values(1,2,'abc');
      select * from db1.test1;
      quit;
      ```

      You can also access it through spark-shell:

      ```
      docker exec -it doris-xx-spark-iceberg spark-shell
      
      spark.sql(s"create database db1")
      spark.sql(s"show databases").show()
      spark.sql(s"create table db1.test1(k1 bigint, k2 bigint, k3 string) partitioned by (k1)").show()
      spark.sql(s"show tables from db1").show()
      spark.sql(s"insert into db1.test1 values(1,2,'abc')").show()
      spark.sql(s"select * from db1.test1").show()
      :q
      ```

      For more usage, please refer to the [Tabular Official Documentation](https://tabular.io/blog/docker-spark-and-iceberg/).

   9. Hudi

      Hudi-related Docker compose files are located in `docker/thirdparties/docker-compose/hudi` directory.

      - `hudi.yaml.tpl`: Docker compose file template, no need to modify.
      - `hadoop.env`: Template for the configuration file, no need to modify.
      - `scripts/` directory: Mounted to the container after startup. The files inside this directory do not need to be modified. However, before starting the container, you need to download the pre-built file: Download the `hudi_docker_compose_attached_file.zip` file from `https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/hudi/hudi_docker_compose_attached_file.zip` and extract it into the `scripts/` directory.

      Before starting, you can add the following settings to `/etc/hosts` to avoid `UnknownHostException` errors:

      ```
      127.0.0.1 adhoc-1
      127.0.0.1 adhoc-2
      127.0.0.1 namenode
      127.0.0.1 datanode1
      127.0.0.1 hiveserver
      127.0.0.1 hivemetastore
      127.0.0.1 sparkmaster
      ```

      After startup, you can start the Hive query using the following command:

      ```
      docker exec -it adhoc-2 /bin/bash
      
      beeline -u jdbc:hive2://hiveserver:10000 \
      --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
      --hiveconf hive.stats.autogather=false
      
      show tables;
      show partitions stock_ticks_mor_rt;
      select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
      select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
      exit;
      ```

      You can also access it through spark-shell:

      ```
      docker exec -it adhoc-1 /bin/bash
      
      $SPARK_INSTALL/bin/spark-shell \
        --jars /var/scripts/hudi_docker_compose_attached_file/jar/hoodie-hive-sync-bundle.jar \
        --master local[2] \
        --driver-class-path $HADOOP_CONF_DIR \
        --conf spark.sql.hive.convertMetastoreParquet=false \
        --deploy-mode client \
        --driver-memory 1G \
        --executor-memory 3G \
        --num-executors 1
      
      spark.sql("show tables").show(100, false)
      spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
      spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)
      spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
      spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
      spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
      :q
      ```

      For more usage, please refer to the [Hudi Official Documentation](https://hudi.apache.org/docs/docker_demo).

   10. Trino

       Trino-related Docker compose files are located in `docker/thirdparties/docker-compose/trino` directory. Template files:

       - `gen_env.sh.tpl`: Used to generate HDFS-related port numbers, no need to modify. If there is a port conflict, you can modify the port numbers.
       - `hive.properties.tpl`: Used to configure Trino catalog information, no need to modify.
       - `trino_hive.env.tpl`: Hive environment configuration information, no need to modify.
       - `trino_hive.yaml.tpl`: Docker compose file, no need to modify.

       When starting the Trino Docker, it will configure a Trino + Hive catalog environment. At this time, Trino has two catalogs:

       1. hive
       2. tpch (built-in with Trino Docker)

       For more usage, please refer to the [Trino Official Documentation](https://trino.io/docs/current/installation/containers.html).

2. Running Regression Tests

   By default, the regression tests related to external tables are disabled. You can modify the following configurations in `regression-test/conf/regression-conf.groovy` to enable them:

   - `enableJdbcTest`: Enable JDBC external table test, requires starting MySQL and Postgresql containers.
   - `mysql_57_port` and `pg_14_port`: Correspond to the external ports of MySQL and Postgresql, default to 3316 and 5442, respectively.
   - `enableHiveTest`: Enable Hive external table test, requires starting the Hive container.
   - `hms_port`: Corresponds to the external port of the Hive metastore, default to 9183.
   - `enableEsTest`: Enable Elasticsearch external table test, requires starting the Elasticsearch container.
   - `es_6_port`, `es_7_port`, `es_8_port`: Ports for ES6, ES7, and ES8, respectively.

