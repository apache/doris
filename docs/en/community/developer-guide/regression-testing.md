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

## Basic Concepts

1. `Suite`: A test case (the file name)
2. `Group`: A test set (the directory that the test case belongs to)
3. `Action`: An encapsulated test action, such as `sql_action`  for SQL execution, `test_action`  for result verification, and `streamLoad_action`  for data ingestion.

## Steps

1. Get the clusters ready
2. Modify the configuration files `${DORIS_HOME}/regression-test/conf/regression-conf.groovy`, set items, such as JDBC URL and user
3. Create the test case files and write the test cases
4. If a test case includes a `qt` Action, you also need to create the relevant data files. For example, the case `suites/demo/qt_action.groovy` will require a TSV file `data/demo/qt_action.out` for output verification.
5. Run `${DORIS_HOME}/run-regression-test.sh` to test all cases, or run `${DORIS_HOME}/run-regression-test.sh --run <suiteName>`  to test a few cases. For more examples, please refer to the "Startup Script Examples" section on this page.

## Directory Structure

Key files and directories to pay attention to:

1. `run-regression-test.sh`: Startup script
2. `regression-conf.groovy`: Default configurations for regression test
3. `data`: where the the input and output data is
4. `suites`: where test cases are

```
./${DORIS_HOME}
    |-- **run-regression-test.sh**           Startup script for regression test
    |-- regression-test
    |   |-- plugins                          Plug-ins
    |   |-- conf
    |   |   |-- logback.xml                  Log configurations
    |   |   |-- **regression-conf.groovy**   Default configurations
    |   |
    |   |-- framework                        framework source code for regression test
    |   |-- **data**                         Input and output data files of test cases
    |   |   |-- demo                         Input and output data files of demo
    |   |   |-- correctness                  Input and output data files of correctness test cases
    |   |   |-- performance                  Input and output data files of performance test cases
    |   |   |-- utils                        Input and output data files of other utilities
    |   |
    |   |-- **suites**                       Test cases for regression testing
    |       |-- demo                         Demo of test cases
    |       |-- correctness                  Test cases of correctness tests
    |       |-- performance                  Test cases of performance tests
    |       |-- utils                        Other utilities
    |
    |-- output
        |-- regression-test
            |-- log                          Logs of regression testing
```


## Default Configurations of the Framework

Modify the JDBC and FE configurations based on the case.

```groovy
/* ============ Key parts to pay attention to ============ */
// The default database, which will be created if the user does not create a database
defaultDb = "regression_test"

// JDBC configurations
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?"
jdbcUser = "root"
jdbcPassword = ""

// FE address configurations, used for Stream Load
feHttpAddress = "127.0.0.1:8030"
feHttpUser = "root"
feHttpPassword = ""

/* ============ Configurations that usually do not require modifications ============ */

// DORIS_HOME is loaded by run-regression-test.sh
// which is java -DDORIS_HOME=./

// Set the path for the test cases
suitePath = "${DORIS_HOME}/regression-test/suites"
// Set the path for the input and output data
dataPath = "${DORIS_HOME}/regression-test/data"
// Set the path for the plug-ins
pluginPath = "${DORIS_HOME}/regression-test/plugins"

// By default, all groups will be read. The groups are separated by comma. For example, "demo,performance".
// This is dynamically specified and overwritten via run-regression-test.sh --run -g
testGroups = ""
// By default, all suites will be read. This is dynamically specified and overwritten via run-regression-test.sh --run -s
testSuites = ""
// Directories specified in this parameter will be loaded by default. It can be dynamically specified and overwritten via run-regression-test.sh --run -d
testDirectories = ""

// Groups specified in this parameter will be excluded. It can be dynamically specified and overwritten via run-regression-test.sh --run -xg
excludeGroups = ""
// Suites specified in this parameter will be excluded. It can be dynamically specified and overwritten via run-regression-test.sh --run -xs
excludeSuites = ""
// Directories specified in this parameter will be excluded. It can be dynamically specified and overwritten via run-regression-test.sh --run -xd
excludeDirectories = ""

// Other self-defined configurations
customConf1 = "test_custom_conf_value"
```

## Steps to Write Test Cases

1. Enter the `${DORIS_HOME}/regression-test` directory
2. Choose a directory based on the test. For correctness test, place the test in `suites/correctness`; for performance test, put the test case in `suites/performance`.
3. Create a groovy test case file, and add a few `Action` for testing. 

## Action

`Action` refers to a test action, which is defined by DSL and provided in the test framework.

### sql action

A `sql_action` is used to commit SQL and obtain results. If the query fails, an error will be thrown.

Parameters:

- String sql: the input SQL string
- `return List<List<Object>>`: the query result. If it is DDL/DML, a result of one row and one column will be returned and the value will be updateRowCount.

You can find the following sample code in `${DORIS_HOME}/regression-test/suites/demo/sql_action.groovy`: 

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

A `qt_action` is used to commit SQL and verify the output results based on the `.out` TSV file.

- String sql: the input SQL string
- return void

You can find the following sample code in `${DORIS_HOME}/regression-test/suites/demo/qt_action.groovy`:

```groovy
suite("qt_action", "demo") {
    /**
     * qt_xxx sql equals to quickTest(xxx, sql) in which xxx is tag.
     * The result will be compared to the relevant file: ${DORIS_HOME}/regression_test/data/qt_action.out.
     *
     * If you want to generate .out tsv file, you can run -genOut or -forceGenOut.
     * e.g
     *   ${DORIS_HOME}/run-regression-test.sh --run qt_action -genOut
     *   ${DORIS_HOME}/run-regression-test.sh --run qt_action -forceGenOut
     */
    qt_select "select 1, 'beijing' union all select 2, 'shanghai'"

    qt_select2 "select 2"

    // Order result by string dict then compare to .out file.
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

A `test_action` is to test with more complicated verification rules, such as to verify the number of rows, execution time, and the existence of errors

Parameters:

- String sql: the input SQL string
- `List<List<Object>> result`: provide a list to check if the output is equal to that list
- `Iterator<Object> resultIterator`: provide an iterator to check if the output is equal to that iterator
- String resultFile: provide a file URI (a relative path of a local file or a http(s) path) to check if the output is equal to the http response stream. The format is similar to that of the `.out` file, but there is no header or comment.
- String exception: check if the error thrown includes certain strings
- long rowNum: check the number of rows
- long time: check if the execution time is shorter than this value, which is measured in millisecond
- `Closure<List<List<Object>>, Throwable, Long, Long> check`: self-defined callback validation function, in which you can input the query result, error thrown, and response time. When there is a callback validation function, other verification methods will fail.

You can find the following sample code in `${DORIS_HOME}/regression-test/suites/demo/test_action.groovy`:

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

An `explain_action` is used to check if the returned string contains certain strings.

Parameters:

- String sql: the SQL that needs to be explained
- String contains: check if certain strings are included. You can check for multiple strings at a time.
- String notContains: check if certain strings are not included. You can check for multiple strings at a time.
- `Closure<String> check`: self-defined callback validation function, by which you can obtain the returned string. When there is a callback validation function, other verification methods will fail.
- `Closure<String, Throwable, Long, Long> check`: self-defined callback validation function, by which you can obtain the error thrown and response time

You can find the following sample code in `${DORIS_HOME}/regression-test/suites/demo/explain_action.groovy`:

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

A `streamLoad_action` is used to ingest data.

Parameters:

- String db: database, set to the defaultDb in regression-conf.groovy
- String table: table name
- String file: the path of the file to be loaded. It can be a relative path under the data directory, or a HTTP URL.
- `Iterator<List<Object>> inputIterator`: the iterator to be loaded
- String inputText: the text to be loaded (rarely used)
- InputStream inputStream: the stream to be loaded (rarely used)
- long time: check if the exection time is shorter than this value, which is measure in millisecond
- void set(String key, String value): set the header of the HTTP request for Stream Load, such as label and columnSeparator.
- `Closure<String, Throwable, Long, Long> check`: self-defined callback validation function, by which you can obtain the returned result, error thrown, and response time. When there is a callback validation function, other verification methods will fail.

You can find the following sample code in `${DORIS_HOME}/regression-test/suites/demo/streamLoad_action.groovy`:

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

Other actions include thread, lazyCheck, events, connect, and selectUnionAll. You can find examples for them in `${DORIS_HOME}/regression-test/suites/demo`.

## Startup Script Examples

```shell
# View parameter descriptions for scripts
./run-regression-test.sh h

# View parameter descriptions for framework
./run-regression-test.sh --run -h

# Test all cases
./run-regression-test.sh 

# Delete test framework compilation results and test logs
./run-regression-test.sh --clean

# Test the suite named "sql_action". Currently, the test case file suffix is the same as the suiteName, so the corresponding test case file in this case is sql_action.groovy.
./run-regression-test.sh --run sql_action 

# Test the suite whose name contains 'sql'. **Remember to use single quotation marks.**
./run-regression-test.sh --run '*sql*' 

# Test the "demo" and "perfomance" group
./run-regression-test.sh --run -g 'demo,performance'

# Test the suite "sql_action" in the "demo" group
./run-regression-test.sh --run -g demo -s sql_action

# Test the suite "sql_action" in the demo directory
./run-regression-test.sh --run -d demo -s sql_action

# Test the suites under the demo directory except the "sql_action" suite
./run-regression-test.sh --run -d demo -xs sql_action

# Exclude the test cases in the demo directory
./run-regression-test.sh --run -xd demo

# Exclude the test cases in the "demo" group
./run-regression-test.sh --run -xg demo

# Self-defined configurations
./run-regression-test.sh --run -conf a=b

# Parallel execution
./run-regression-test.sh --run -parallel 5 -suiteParallel 10 -actionParallel 20
```

## Generate `.out` File Based on the Query Result

```shell
# Automatically generate an .out file for the sql_action test case based on the query result. Ignore this if the .out file already exists.
./run-regression-test.sh --run sql_action -genOut

# Automatically generate an .out file for the sql_action test case based on the query result. If an .out file already exist, it will be overwritten.
./run-regression-test.sh --run sql_action -forceGenOut
```

## Suite Plug-in

Sometimes there might be a need to expand the suite classes, but it is not easy to change the source code of suite classes. In this case, you might do that by plug-ins. The default directory for plug-ins is `${DORIS_HOME}/regression-test/plugins`, in whic you can define how to expand suite classes with a groovy script. For example, this is how to add a `testPlugin` function for log printing to `plugin_example.groovy`:

```groovy
import org.apache.doris.regression.suite.Suite

// Register `testPlugin` function to Suite,
// and invoke in ${DORIS_HOME}/regression-test/suites/demo/test_plugin.groovy
Suite.metaClass.testPlugin = { String info /* param */ ->

    // Which suite invoke current function?
    Suite suite = delegate as Suite

    // Function body
    suite.getLogger().info("Test plugin: suiteName: ${suite.name}, info: ${info}".toString())

    // Optional return value
    return "OK"
}

logger.info("Added 'testPlugin' function to Suite")
```

After adding the `testPlugin` function, you can use it in a regular test case. For example, in `${DORIS_HOME}/regression-test/suites/demo/test_plugin.groovy`:

```groovy
suite("test_plugin", "demo") {
    // register testPlugin function in ${DORIS_HOME}/regression-test/plugins/plugin_example.groovy
    def result = testPlugin("message from suite")
    assertEquals("OK", result)
}
```

## CI/CD Support

### TeamCity

You can use TeamCity to recognize Service Message via stdout. If you start the regression test framework using the `--teamcity` parameter, the TeamCity Service Message will be printed in stdout. TeamCity will automatically read the event logs in stdout, and show `Tests` in the current pipeline, in which there will be the test and logs. Therefore, you only need to configure the following command to start the regression test framework. In the following snippet, `-Dteamcity.enableStdErr=false` means to print error logs to stdout, too, so that the logs can be chronologically organized.

```shell
JAVA_OPTS="-Dteamcity.enableStdErr=${enableStdErr}" ./run-regression-test.sh --teamcity --run
```

## E2E Test with External Data Sources

Doris supports queries on external data sources, so the regression testing framework allows users to build external data sources using Docker Compose, so that they can run end-to-end tests with external data sources. 

0. Preparation

   To begin with, modify `CONTAINER_UID` in `docker/thirdparties/custom_settings.env`. For example, `doris-10002-18sda1-`. The follow-up startup scripts will replace the corresponding names in Docker Compose to ensure consistency across multiple containers environment.

1. Start the Container

   So far, Doris has supported Docker Compose for data sources including Elasticsearch, MySQL, PostgreSQL, Hive, SQLServer, Oracle, Iceberg, Hudi, and Trino. The relevant files can be found in the directory `docker/thirdparties/docker-compose`.

   By default, you can use the following command to start the Docker containers for all external data sources:

   (Note that for Hive and Hudi containers, you also need to download the pre-built data files. See the relevant documentation of Hive and Hudi.)

   ```
   cd docker/thirdparties && sh run-thirdparties-docker.sh
   ```

   Executing this command requires root or sudo privilege. If the command returns, that means all containers are started. You can check on them by inputing a `docker ps -a` command. 

   To stop all containers, you can use the following command:

   ```
   cd docker/thirdparties && sh run-thirdparties-docker.sh --stop
   ```

   To start or stop some specific components, you can use the following commands:

   ```
   cd docker/thirdparties
   # Start MySQL
   sh run-thirdparties-docker.sh -c mysql
   # Start MySQL, PostgreSQL, Iceberg
   sh run-thirdparties-docker.sh -c mysql,pg,iceberg
   # Stop MySQL, PostgreSQL, Iceberg
   sh run-thirdparties-docker.sh -c mysql,pg,iceberg --stop
   ```

   1. MySQL

      You can find the MySQL-related Docker Compose files in `docker/thirdparties/docker-compose/mysql` .

      * `mysql-5.7.yaml.tpl`: Docker Compose file template, no modification required. The default username and password is root/123456
      * `mysql-5.7.env`: Configuration file, in which you can configure the external port of the MySQL container (default number: 3316).
      * `init/`: SQL files in this directory will be automatically executed once the container is created.
      * `data/`: The container will be mounted to this local data directory after it is started. The `run-thirdparties-docker.sh`  script will clear and rebuild this directory every time it is started.

   2. PostgreSQL

      You can find the PostgreSQL-related Docker Compose files in  `docker/thirdparties/docker-compose/postgresql` .

      * `postgresql-14.yaml.tpl`: Docker Compose file template, no modification required. The default username and password is postgres/123456
      * `postgresql-14.env`: Configuration file, in which you can configure the external port of the PostgreSQL container (default number: 5442).
      * `init/`: SQL files in this directory will be automatically executed once the container is created. By default, that includes the creation of database, table, and some data input.
      * `data/`: The container will be mounted to this local data directory after it is started. The `run-thirdparties-docker.sh`  script will clear and rebuild this directory every time it is started.

   3. Hive

      You can find the Hive-related Docker Compose files in `docker/thirdparties/docker-compose/hive` .

      * `hive-2x.yaml.tpl`: Docker Compose file template, no modification required.

      * `hadoop-hive.env.tpl`: The configuration file template, no modification required.

      * `gen_env.sh`: The script for initializing the configuration file. In this script, you can modify the two external ports: `FS_PORT` for defaultFs and `HMS_PORT` for Hive metastore (default numbers: 8120 and 9183, respectively). This script will be called once `run-thirdparties-docker.sh`  is started.

      * The `scripts/` directory will be mounted to the container once it is started. Files in this directory require no modifications. Note that you need to download the pre-built files before you start the container: 

        Download files from  `https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/tpch1_parquet/tpch1.db.tar.gz`  to the `scripts/` directory and decompress.

   4. Elasticsearch

      You can find three Docker images (for Elasticsearch 6, Elasticsearch 7, and Elasticsearch 8) in `docker/thirdparties/docker-compose/elasticsearch/` .

      * `es.yaml.tpl`: Docker Compose file template, three versions included, no modifications required.
      * `es.env`: Configuration file, in which you need to configure the port number for Elasticsearch. 
      * `scripts` : In this directory, you can find the initialization script after the image is started.

   5. Oracle

      You can find the image for Oracle 11 in `docker/thirdparties/docker-compose/oracle/`.

      * `oracle-11.yaml.tpl`: Docker Compose file template, no modifications required. 
      * `oracle-11.env`: Configure the external port for Oracle (default number: 1521).

   6. SQLServer

      You can find the image for SQLServer 2022 in `docker/thirdparties/docker-compose/sqlserver/` .

      * `sqlserver.yaml.tpl`: Docker Compose file template, no modifications required. 
      * `sqlserver.env`: Configure the external port for SQLServer (default number: 1433).

   7. ClickHouse

      You can find the image for ClickHouse 22 in `docker/thirdparties/docker-compose/clickhouse/`.

       * `clickhouse.yaml.tpl`: Docker Compose file template, no modifications required.
       * `clickhouse.env`: Configure the external port for ClickHouse (default number: 8123).

   8. Iceberg

      You can find the image of Iceberg + Spark + Minio in `docker/thirdparties/docker-compose/iceberg/`.

      * `iceberg.yaml.tpl`: Docker Compose file template, no modifications required.
      * `entrypoint.sh.tpl`: The template of the initialization script after the image is started, no modifications required.
      * `spark-defaults.conf.tpl`: Configuration file template for Spark, no modifications required.
      * `iceberg.env`: Configuration file for external ports. You need to modify every external port to avoid conflicts.

      After the image is started, execute the following command to start spark-sql:

      `docker exec -it doris-xx-spark-iceberg spark-sql`        

      In this command, `doris-xx-spark-iceberg` is the container name.

      Execution examples for spark-sql iceberg:

      ```
      create database db1;
      show databases;
      create table db1.test1(k1 bigint, k2 bigint, k3 string) partitioned by (k1);
      insert into db1.test1 values(1,2,'abc');
      select * from db1.test1;
      quit;
      ```

      You can also access by spark-shell:

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

      For more usage guide, see  [Tabular Documentation](https://tabular.io/blog/docker-spark-and-iceberg/).

   9. Hudi

      You can find the Hudi-related Docker Compose file in `docker/thirdparties/docker-compose/hudi`.

      * `hudi.yaml.tpl`: Docker Compose file template, no modifications required.

      * `hadoop.env`: Configuration file template, no modifications required.

      * The `scripts/` directory will be mounted to the container once it is started. Files in this directory require no modifications. Note that you need to download the pre-built files before you start the container: 

        Download files from `https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/load/hudi/hudi_docker_compose_attached_file.zip`  to the `scripts/` directory and decompress.

      * Before starting, add the following configurations to `/etc/hosts` to avoid `UnknownHostException`:

      ```
      127.0.0.1 adhoc-1
      127.0.0.1 adhoc-2
      127.0.0.1 namenode
      127.0.0.1 datanode1
      127.0.0.1 hiveserver
      127.0.0.1 hivemetastore
      127.0.0.1 sparkmaster
      ```

      After starting, you can execute the following command to start a Hive query:

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

      You can also access by spark-shell:

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

      For more usage guide, see [Hudi Documentation](https://hudi.apache.org/docs/docker_demo).

   10. Trino
       You can find the Trino-related Docker Compose file in `docker/thirdparties/docker-compose/trino`.

       Template files:

       * gen_env.sh.tpl: This is used to generate HDFS-related port numbers, no modifications required, but you can change the port numbers in the case of port conflicts.

       * hive.properties.tpl: This is used to configure the Trino catalog, no modifications required. 

       * trino_hive.env.tpl: Environment configurations of Hive, no modifications required. 

       * trino_hive.yaml.tpl: Docker Compose file, no modifications required. 

         After the Trino Docker is started, a Trino + Hive Catalog will be configured, and then Trino will have two catalogs.

       1. Hive
       2. TPCH (self-contained in Trino Docker)

       For more usage guide, see  [Trino Documentation](https://trino.io/docs/current/installation/containers.html).

2. Run the regression test

   Regression test for external data sources is disabled by default. You can enable it by modifying the following configurations in  `regression-test/conf/regression-conf.groovy` :

   * `enableJdbcTest`: This is to enable test for JDBC external tables. For this purpose, you need to start the MySQL and PostgreSQL containers.
   * `mysql_57_port` and `pg_14_port` are the external port of MySQL and PostgreSQL, respectively. Default port numbers: 3316 and 5442.
   * `enableHiveTest`: This is to enable test for Hive external tables. For this purpose, you need to start the Hive container.
   * `hms_port` is the external port for Hive metastore. Default number: 9183.
   * `enableEsTest`: This is to enable test for Elasticsearch external tables. For this purpose, you need to start the Elasticsearch container.
   * `es_6_port`: Port for Elasticsearch 6.
   * `es_7_port`: Port for Elasticsearch 7.
   * `es_8_port`: Port for Elasticsearch 8.
