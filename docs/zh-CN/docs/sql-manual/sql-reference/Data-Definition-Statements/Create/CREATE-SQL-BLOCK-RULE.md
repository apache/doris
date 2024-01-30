---
{
    "title": "CREATE-SQL-BLOCK-RULE",
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

## CREATE-SQL-BLOCK-RULE

### Name

CREATE SQL BLOCK RULE

### Description

该语句创建SQL阻止规则，该功能可用于限制任何sql语句（包括 DDL 和 DML 语句）。

支持按用户配置SQL黑名单：

- 通过正则匹配的方式拒绝指定SQL
- 通过设置partition_num, tablet_num, cardinality, 检查一个查询是否达到其中一个限制
  - partition_num, tablet_num, cardinality 可以一起设置，一旦一个查询达到其中一个限制，查询将会被拦截

语法：

```sql
CREATE SQL_BLOCK_RULE rule_name 
[PROPERTIES ("key"="value", ...)];
```

参数说明：

- sql：匹配规则(基于正则匹配,特殊字符需要转译,如`select *`使用`select \\*`)，可选，默认值为 "NULL", 最后不要带分号
- sqlHash: sql hash值，用于完全匹配，我们会在`fe.audit.log`打印这个值，可选，这个参数和sql只能二选一，默认值为 "NULL"
- partition_num: 一个扫描节点会扫描的最大partition数量，默认值为0L
- tablet_num: 一个扫描节点会扫描的最大tablet数量，默认值为0L
- cardinality: 一个扫描节点粗略的扫描行数，默认值为0L
- global：是否全局(所有用户)生效，默认为false
- enable：是否开启阻止规则，默认为true

### Example

1. 创建一个名称为 test_rule 的阻止规则

     ```sql
     CREATE SQL_BLOCK_RULE test_rule 
     PROPERTIES(
       "sql"="select \\* from order_analysis",
       "global"="false",
       "enable"="true"
     );
     ```
    当我们去执行刚才我们定义在规则里的sql时就会返回异常错误，示例如下：

     ```sql
     mysql> select * from order_analysis;
     ERROR 1064 (HY000): errCode = 2, detailMessage = sql match regex sql block rule: order_analysis_rule
     ```

2. 创建 test_rule2，将最大扫描的分区数量限制在30个，最大扫描基数限制在100亿行，示例如下：

    ```sql
    CREATE SQL_BLOCK_RULE test_rule2 
    PROPERTIES
    (
       "partition_num" = "30",
       "cardinality" = "10000000000",
       "global" = "false",
       "enable" = "true"
    );
   Query OK, 0 rows affected (0.01 sec)
   ```
   
3. 创建包含特殊字符的 SQL BLOCK RULE， 正则表达式中 ( 和 ) 符号是特殊符号，所以需要转义，示例如下：

    ```sql
    CREATE SQL_BLOCK_RULE test_rule3
    PROPERTIES
    ( 
    "sql" = "select count\\(1\\) from db1.tbl1"
    );
    CREATE SQL_BLOCK_RULE test_rule4
    PROPERTIES
    ( 
    "sql" = "select \\* from db1.tbl1"
    );
    ```
   
4. SQL_BLCOK_RULE 中，SQL 的匹配是基于正则的，如果想匹配更多模式的 SQL 需要写相应的正则，比如忽略 SQL
中空格，还有 order_ 开头的表都不能查询，示例如下：

   ```sql
     CREATE SQL_BLOCK_RULE test_rule4 
     PROPERTIES(
       "sql"="\\s*select\\s*\\*\\s*from order_\\w*\\s*",
       "global"="false",
       "enable"="true"
     );
   ```

### 附录
常用正则表达式如下：

>     . ：匹配任何单个字符，除了换行符 \n。
> 
>     * ：匹配前面的元素零次或多次。例如，a* 匹配零个或多个 'a'。
>
>     + ：匹配前面的元素一次或多次。例如，a+ 匹配一个或多个 'a'。
>
>     ? ：匹配前面的元素零次或一次。例如，a? 匹配零个或一个 'a'。
>
>     [] ：用于定义字符集合。例如，[aeiou] 匹配任何一个元音字母。
>
>     [^] ：在字符集合中使用 ^ 表示否定，匹配不在集合内的字符。例如，[^0-9] 匹配任何非数字字符。
>
>     () ：用于分组表达式，可以对其应用量词。例如，(ab)+ 匹配连续的 'ab'。
>
>     | ：用于表示或逻辑。例如，a|b 匹配 'a' 或 'b'。
>
>     ^ ：匹配字符串的开头。例如，^abc 匹配以 'abc' 开头的字符串。
>
>     $ ：匹配字符串的结尾。例如，xyz$ 匹配以 'xyz' 结尾的字符串。
>
>     \ ：用于转义特殊字符，使其变成普通字符。例如，\\. 匹配句点字符 '.'。
>
>     \s ：匹配任何空白字符，包括空格、制表符、换行符等。
>
>     \d ：匹配任何数字字符，相当于 [0-9]。
>
>     \w ：匹配任何单词字符，包括字母、数字和下划线，相当于 [a-zA-Z0-9_]。

### Keywords

```text
CREATE, SQL_BLCOK_RULE
```

### Best Practice

