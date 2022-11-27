---
{
"title": "Java UDF",
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

# Java UDF

<version since="1.2">

Java UDF 为用户提供UDF编写的Java接口，以方便用户使用Java语言进行自定义函数的执行。相比于 Native 的 UDF 实现，Java UDF 有如下优势和限制：
1. 优势
* 兼容性：使用Java UDF可以兼容不同的Doris版本，所以在进行Doris版本升级时，Java UDF不需要进行额外的迁移操作。与此同时，Java UDF同样遵循了和Hive/Spark等引擎同样的编程规范，使得用户可以直接将Hive/Spark的UDF jar包迁移至Doris使用。
* 安全：Java UDF 执行失败或崩溃仅会导致JVM报错，而不会导致 Doris 进程崩溃。
* 灵活：Java UDF 中用户通过把第三方依赖打进用户jar包，而不需要额外处理引入的三方库。

2. 使用限制
* 性能：相比于 Native UDF，Java UDF会带来额外的JNI开销，不过通过批式执行的方式，我们已经尽可能的将JNI开销降到最低。
* 向量化引擎：Java UDF当前只支持向量化引擎。

</version>

### 类型对应关系

|Type|UDF Argument Type|
|----|---------|
|Bool|Boolean|
|TinyInt|Byte|
|SmallInt|Short|
|Int|Integer|
|BigInt|Long|
|LargeInt|BigInteger|
|Float|Float|
|Double|Double|
|Date|LocalDate|
|Datetime|LocalDateTime|
|Char|String|
|Varchar|String|
|Decimal|BigDecimal|

## 编写 UDF 函数

本小节主要介绍如何开发一个 Java UDF。在 `samples/doris-demo/java-udf-demo/` 下提供了示例，可供参考，查看点击[这里](https://github.com/apache/doris/tree/master/samples/doris-demo/java-udf-demo)

使用Java代码编写UDF，UDF的主入口必须为 `evaluate` 函数。这一点与Hive等其他引擎保持一致。在本示例中，我们编写了 `AddOne` UDF来完成对整型输入进行加一的操作。
值得一提的是，本例不只是Doris支持的Java UDF，同时还是Hive支持的UDF，也就是说，对于用户来讲，Hive UDF是可以直接迁移至Doris的。

## 创建 UDF

```sql
CREATE FUNCTION 
name ([,...])
[RETURNS] rettype
PROPERTIES (["key"="value"][,...])	
```
说明：

1. PROPERTIES中`symbol`表示的是包含UDF类的类名，这个参数是必须设定的。
2. PROPERTIES中`file`表示的包含用户UDF的jar包，这个参数是必须设定的。
3. PROPERTIES中`type`表示的 UDF 调用类型，默认为 Native，使用 Java UDF时传 JAVA_UDF。
4. PROPERTIES中`always_nullable`表示的 UDF 返回结果中是否有可能出现NULL值，是可选参数，默认值为true。
5. name: 一个function是要归属于某个DB的，name的形式为`dbName`.`funcName`。当`dbName`没有明确指定的时候，就是使用当前session所在的db作为`dbName`。

示例：
```sql
CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
    "file"="file:///path/to/java-udf-demo-jar-with-dependencies.jar",
    "symbol"="org.apache.doris.udf.AddOne",
    "always_nullable"="true",
    "type"="JAVA_UDF"
);
```
* "file"="http://IP:port/udf-code.jar", 当在多机环境时，也可以使用http的方式下载jar包
* "always_nullable"可选属性, 如果在计算中对出现的NULL值有特殊处理，确定结果中不会返回NULL，可以设为false，这样在整个查询计算过程中性能可能更好些。

## 编写 UDAF 函数
<br/>

在使用Java代码编写UDAF时，有一些必须实现的函数(标记required)和一个内部类State，下面将以一个具体的实例来说明
下面的SimpleDemo将实现一个类似的sum的简单函数,输入参数INT，输出参数是INT
```JAVA
package org.apache.doris.udf;

public class SimpleDemo {
    //Need an inner class to store data
    /*required*/  
    public static class State {
        /*some variables if you need */
        public int sum = 0;
    }

    /*required*/
    public State create() {
        /* here could do some init work if needed */
        return new State();
    }

    /*required*/
    public void destroy(State state) {
      /* here could do some destroy work if needed */
    }

    /*required*/ 
    //first argument is State, then other types your input
    public void add(State state, Integer val) {
      /* here doing update work when input data*/
        if (val != null) {
            state.sum += val;
        }
    }

    /*required*/
    public void serialize(State state, DataOutputStream out) {
      /* serialize some data into buffer */
        out.writeInt(state.sum);
    }

    /*required*/
    public void deserialize(State state, DataInputStream in) {
      /* deserialize get data from buffer before you put */
        int val = in.readInt();
        state.sum = val;
    }

    /*required*/
    public void merge(State state, State rhs) {
      /* merge data from state */
        state.sum += rhs.sum;
    }

    /*required*/
    //return Type you defined
    public Integer getValue(State state) {
      /* return finally result */
        return state.sum;
    }
}

```

```sql
CREATE AGGREGATE FUNCTION simple_sum(int) RETURNS int PROPERTIES (
    "file"="file:///pathTo/java-udaf.jar",
    "symbol"="org.apache.doris.udf.SimpleDemo",
    "always_nullable"="true",
    "type"="JAVA_UDF"
);
```
* 实现的jar包可以放在本地也可以存放在远程服务端通过http下载，但必须让每个BE节点都能获取到jar包;
否则将会返回错误状态信息"Couldn't open file ......".

目前还暂不支持UDTF

<br/>

## 使用 UDF

用户使用 UDF 必须拥有对应数据库的 `SELECT` 权限。

UDF 的使用与普通的函数方式一致，唯一的区别在于，内置函数的作用域是全局的，而 UDF 的作用域是 DB内部。当链接 session 位于数据内部时，直接使用 UDF 名字会在当前DB内部查找对应的 UDF。否则用户需要显示的指定 UDF 的数据库名字，例如 `dbName`.`funcName`。

## 删除 UDF

当你不再需要 UDF 函数时，你可以通过下述命令来删除一个 UDF 函数, 可以参考 `DROP FUNCTION`。

## 示例
在`samples/doris-demo/java-udf-demo/` 目录中提供了具体示例。具体使用方法见每个目录下的`README.md`，查看点击[这里](https://github.com/apache/doris/tree/master/samples/doris-demo/java-udf-demo)

## 使用须知
1. 不支持复杂数据类型（HLL，Bitmap）。
2. 当前允许用户自己指定JVM最大堆大小，配置项是jvm_max_heap_size。
3. char类型的udf在create function时需要使用String类型。
4. 由于jvm加载同名类的问题，不要同时使用多个同名类作为udf实现，如果想更新某个同名类的udf，需要重启be重新加载classpath。

