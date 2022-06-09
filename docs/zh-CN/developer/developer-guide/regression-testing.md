---
{
    "title": "回归测试",
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

# 回归测试

## 概念
1. `Suite`: 一个测试用例，目前仅用来指代测试用例文件名
2. `Group`: 一个测试集，目前仅用于指代测试用例所属的目录
3. `Action`: 一个封装好的具体测试行为，比如用于执行sql的`sql` Action，用于校验结果的`test` Action，用于导入数据的`streamLoad` Action等

## 测试步骤
1. 需要预先安装好集群
2. 修改配置文件`${DORIS_HOME}/regression-test/conf/regression-conf.groovy`，设置jdbc url、用户等配置项
3. 创建测试用例文件并编写用例
4. 如果用例文件包含`qt` Action，则需要创建关联的data文件，比如`suites/demo/qt_action.groovy`这个例子，需要用到`data/demo/qt_action.out`这个TSV文件来校验输出是否一致
5. 运行`${DORIS_HOME}/run-regression-test.sh`测试全部用例,或运行`${DORIS_HOME}/run-regression-test.sh --run <suiteName>` 测试若干用例，更多例子见"启动脚本例子"章节

## 目录结构
开发时需要关注的重要文件/目录
1. `run-regression-test.sh`: 启动脚本
2. `regression-conf.groovy`: 回归测试的默认配置
3. `data`: 存放输入数据和输出校验数据
4. `suites`: 存放用例

```
./${DORIS_HOME}
    |-- **run-regression-test.sh**           回归测试启动脚本
    |-- regression-test
    |   |-- plugins                          插件目录
    |   |-- conf
    |   |   |-- logback.xml                  日志配置文件
    |   |   |-- **regression-conf.groovy**   默认配置文件
    |   |
    |   |-- framework                        回归测试框架源码
    |   |-- **data**                         用例的输入输出文件
    |   |   |-- demo                         存放demo的输入输出文件
    |   |   |-- correctness                  存放正确性测试用例的输入输出文件
    |   |   |-- performance                  存放性能测试用例的输入输出文件
    |   |   |-- utils                        存放其他工具的输入输出文件
    |   |
    |   |-- **suites**                       回归测试用例
    |       |-- demo                         存放测试用例的demo
    |       |-- correctness                  存放正确性测试用例
    |       |-- performance                  存放性能测试用例
    |       |-- utils                        其他工具
    |
    |-- output
        |-- regression-test
            |-- log                          回归测试日志
```


## 框架默认配置
测试时需要实际情况修改jdbc和fe的配置
```groovy

/* ============ 一般只需要关注下面这部分 ============ */
// 默认DB,如果未创建,则会尝试创建这个db
defaultDb = "regression_test"

// Jdbc配置
jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?"
jdbcUser = "root"
jdbcPassword = ""

// fe地址配置, 用于stream load
feHttpAddress = "127.0.0.1:8030"
feHttpUser = "root"
feHttpPassword = ""

/* ============ 一般不需要修改下面的部分 ============ */

// DORIS_HOME变量是通过run-regression-test.sh传入的
// 即 java -DDORIS_HOME=./

// 设置回归测试用例的目录
suitePath = "${DORIS_HOME}/regression-test/suites"
// 设置输入输出数据的目录
dataPath = "${DORIS_HOME}/regression-test/data"
// 设置插件的目录
pluginPath = "${DORIS_HOME}/regression-test/plugins"

// 默认会读所有的组,读多个组可以用半角逗号隔开，如: "demo,performance"
// 一般不需要在配置文件中修改，而是通过run-regression-test.sh --run -g来动态指定和覆盖
testGroups = ""
// 默认会读所有的用例, 同样可以使用run-regression-test.sh --run -s来动态指定和覆盖
testSuites = ""
// 默认会加载的用例目录, 可以通过run-regression-test.sh --run -d来动态指定和覆盖
testDirectories = ""

// 排除这些组的用例，可通过run-regression-test.sh --run -xg来动态指定和覆盖
excludeGroups = ""
// 排除这些suite，可通过run-regression-test.sh --run -xs来动态指定和覆盖
excludeSuites = ""
// 排除这些目录，可通过run-regression-test.sh --run -xd来动态指定和覆盖
excludeDirectories = ""

// 其他自定义配置
customConf1 = "test_custom_conf_value"
```

## 编写用例的步骤
1. 进入`${DORIS_HOME}/regression-test`目录
2. 根据测试的目的来选择用例的目录，正确性测试存在`suites/correctness`，而性能测试存在`suites/performance`
3. 新建一个groovy用例文件，增加若干`Action`用于测试，Action将在后续章节具体说明

## Action
Action是一个测试框架默认提供的测试行为，使用DSL来定义。

### sql action
sql action用于提交sql并获取结果，如果查询失败则会抛出异常

参数如下
- String sql: 输入的sql字符串
- return List<List<Object>>: 查询结果，如果是DDL/DML，则返回一行一列，唯一的值是updateRowCount

下面的样例代码存放于`${DORIS_HOME}/regression-test/suites/demo/sql_action.groovy`:
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
qt action用于提交sql，并使用对应的.out TSV文件来校验结果
- String sql: 输入sql字符串
- return void

下面的样例代码存放于`${DORIS_HOME}/regression-test/suites/demo/qt_action.groovy`:
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
test action可以使用更复杂的校验规则来测试，比如验证行数、执行时间、是否抛出异常

可用参数
- String sql: 输入的sql字符串
- List<List<Object>> result: 提供一个List对象，用于比较真实查询结果与List对象是否相等
- Iterator<Object> resultIterator: 提供一个Iterator对象，用于比较真实查询结果与Iterator是否相等
- String resultFile: 提供一个文件Uri(可以是本地文件相对路径，或http(s)路径)，用于比较真实查询结果与http响应流是否相等，格式与.out文件格式类似，但没有块头和注释
- String exception: 校验抛出的异常是否包含某些字符串
- long rowNum: 验证结果行数
- long time: 验证执行时间是否小于这个值，单位是毫秒
- Closure<List<List<Object>>, Throwable, Long, Long> check: 自定义回调校验，可传入结果、异常、时间。存在回调函数时，其他校验方式会失效。

下面的样例代码存放于`${DORIS_HOME}/regression-test/suites/demo/test_action.groovy`:
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
explain action用来校验explain返回的字符串是否包含某些字符串

可用参数:
- String sql: 查询的sql，需要去掉sql中的explain
- String contains: 校验explain是否包含某些字符串，可多次调用校验同时多个结果
- String notContains: 校验explain是否不含某些字符串，可多次调用校验同时多个结果
- Closure<String> check: 自定义校验回调函数，可以获取返回的字符串，存在校验函数时，其他校验方式会失效
- Closure<String, Throwable, Long, Long> check: 自定义校验回调函数，可以额外获取异常和时间

下面的样例代码存放于`${DORIS_HOME}/regression-test/suites/demo/explain_action.groovy`:
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
streamLoad action用于导入数据
可用参数为
- String db: db，默认值为regression-conf.groovy中的defaultDb
- String table: 表名
- String file: 要导入的文件路径，可以写data目录下的相对路径，或者写http url来导入网络文件
- Iterator<List<Object>> inputIterator: 要导入的迭代器
- String inputText: 要导入的文本, 较为少用
- InputStream inputStream: 要导入的字节流，较为少用
- long time: 验证执行时间是否小于这个值，单位是毫秒
- void set(String key, String value): 设置stream load的http请求的header，如label、columnSeparator
- Closure<String, Throwable, Long, Long> check: 自定义校验回调函数，可以获取返回结果、异常和超时时间。当存在回调函数时，其他校验项会失效。

下面的样例代码存放于`${DORIS_HOME}/regression-test/suites/demo/streamLoad_action.groovy`:
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

### 其他Action
thread, lazyCheck, events, connect, selectUnionAll
具体可以在这个目录找到例子: `${DORIS_HOME}/regression-test/suites/demo`

## 启动脚本例子
```shell
# 查看脚本参数说明
./run-regression-test.sh h

# 查看框架参数说明
./run-regression-test.sh --run -h

# 测试所有用例
./run-regression-test.sh 

# 删除测试框架编译结果和测试日志
./run-regression-test.sh --clean

# 测试suiteName为sql_action的用例, 目前suiteName等于文件名前缀，例子对应的用例文件是sql_action.groovy
./run-regression-test.sh --run sql_action 

# 测试suiteName包含'sql'的用例，**注意需要用单引号括起来**
./run-regression-test.sh --run '*sql*' 

# 测试demo和perfomance group
./run-regression-test.sh --run -g 'demo,performance'

# 测试demo group下的sql_action
./run-regression-test.sh --run -g demo -s sql_action

# 测试demo目录下的sql_action
./run-regression-test.sh --run -d demo -s sql_action

# 测试demo目录下用例，排除sql_action用例
./run-regression-test.sh --run -d demo -xs sql_action

# 排除demo目录的用例
./run-regression-test.sh --run -xd demo

# 排除demo group的用例
./run-regression-test.sh --run -xg demo

# 自定义配置
./run-regression-test.sh --run -conf a=b

# 并发执行
./run-regression-test.sh --run -parallel 5 -suiteParallel 10 -actionParallel 20
```

## 使用查询结果自动生成.out文件
```shell
# 使用查询结果自动生成sql_action用例的.out文件，如果.out文件存在则忽略
./run-regression-test.sh --run sql_action -genOut

# 使用查询结果自动生成sql_action用例的.out文件，如果.out文件存在则覆盖
./run-regression-test.sh --run sql_action -forceGenOut
```

## Suite插件
有的时候我们需要拓展Suite类，但不便于修改Suite类的源码，则可以通过插件来进行拓展。默认插件目录为`${DORIS_HOME}/regression-test/plugins`，在其中可以通过groovy脚本定义拓展方法，以`plugin_example.groovy`为例，为Suite类增加了testPlugin函数用于打印日志：
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

增加了testPlugin函数后，则可以在普通用例中使用它，以`${DORIS_HOME}/regression-test/suites/demo/test_plugin.groovy`为例:
```groovy
suite("test_plugin", "demo") {
    // register testPlugin function in ${DORIS_HOME}/regression-test/plugins/plugin_example.groovy
    def result = testPlugin("message from suite")
    assertEquals("OK", result)
}
```

## CI/CD的支持
### TeamCity
TeamCity可以通过stdout识别Service Message。当使用`--teamcity`参数启动回归测试框架时，回归测试框架就会在stdout打印TeamCity Service Message，TeamCity将会自动读取stdout中的事件日志，并在当前流水线中展示`Tests`，其中会展示测试的test及其日志。
因此只需要配置下面一行启动回归测试框架的命令即可。其中`-Dteamcity.enableStdErr=false`可以让错误日志也打印到stdout中，方便按时间顺序分析日志。
```shell
JAVA_OPTS="-Dteamcity.enableStdErr=${enableStdErr}" ./run-regression-test.sh --teamcity --run
```