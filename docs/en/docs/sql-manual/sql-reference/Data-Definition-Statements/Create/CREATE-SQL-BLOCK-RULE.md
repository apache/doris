---
{
    "title": "CREATE-SQL-BLOCK-RULE",
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

## CREATE-SQL-BLOCK-RULE

### Name

CREATE SQL BLOCK RULE

### Description

This statement creates a SQL blocking rule. it can restrict any kind of sql statements(no matter DDL or DML statement).

Supports configuring SQL blacklists by user:

- Refuse to specify SQL by regular matching
- Check if a sql reaches one of these limits by setting partition_num, tablet_num, cardinality
  - partition_num, tablet_num, cardinality can be set together, once a query reaches one of these limits, the query will be intercepted

grammar:

```sql
CREATE SQL_BLOCK_RULE rule_name
[PROPERTIES ("key"="value", ...)];
````

Parameter Description:

- sql: matching rule (based on regular matching, special characters need to be translated,for example`select *`use`select \\*`), optional, the default value is "NULL"
- sqlHash: sql hash value, used for exact matching, we will print this value in `fe.audit.log`, optional, this parameter and sql can only be selected one, the default value is "NULL"
- partition_num: the maximum number of partitions a scan node will scan, the default value is 0L
- tablet_num: The maximum number of tablets that a scanning node will scan, the default value is 0L
- cardinality: the rough scan line number of a scan node, the default value is 0L
- global: Whether to take effect globally (all users), the default is false
- enable: whether to enable blocking rules, the default is true

### Example

1. Create a block rule named test_rule

    ```sql
    CREATE SQL_BLOCK_RULE test_rule
    PROPERTIES(
    "sql"="select \\* from order_analysis",
    "global"="false",
    "enable"="true"
    );
    ````

    >Notes:
    >
    >That the sql statement here does not end with a semicolon
    
    When we execute the sql we just defined in the rule, an exception error will be returned. The example is as follows:
    
    ```sql
    select * from order_analysis;
    ERROR 1064 (HY000): errCode = 2, detailMessage = sql match regex sql block rule: order_analysis_rule
    ````


2. Create test_rule2, limit the maximum number of scanned partitions to 30, and limit the maximum scan base to 10 billion rows. The example is as follows:

   ```sql
    CREATE SQL_BLOCK_RULE test_rule2
    PROPERTIES (
    "partition_num" = "30",
    "cardinality" = "10000000000",
    "global" = "false",
    "enable" = "true"
    );
   ````
3. Create SQL BLOCK RULE with special chars

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
4. In SQL_BLOCK_RULE, SQL matching is based on regular expressions. If want to match more patterns of SQL, need to write the corresponding regex. For example, to ignore spaces in SQL and not query tables that start with 'order_', as shown below:   

    ```sql
     CREATE SQL_BLOCK_RULE test_rule4 
     PROPERTIES(
       "sql"="\\s*select\\s*\\*\\s*from order_\\w*\\s*",
       "global"="false",
       "enable"="true"
     );
    ```

### APPENDIX
Here are some commonly used regular expressions:
>     . ：Matches any single character except for a newline character \n.
>
>     * ：Matches the preceding element zero or more times. For example, a matches zero or more 'a'.
>
>     + ：Matches the preceding element one or more times. For example, a+ matches one or more 'a'.
>
>     ? ：Matches the preceding element zero or one time. For example, a? matches zero or one 'a'.
>
>     [] ：Used to define a character set. For example, [aeiou] matches any one vowel letter.
>
>     [^] ：In a character set, use ^ to indicate negation, matching characters that are not in the set. For example, [^0-9] matches any non-digit character.
>
>     () ：Used for grouping expressions and applying quantifiers. For example, (ab)+ matches consecutive 'ab'.
>
>     | ：Represents logical OR. For example, a|b matches 'a' or 'b'.
>
>     ^ ：Matches the beginning of a string. For example, ^abc matches a string that starts with 'abc'.
>
>     $ ：Matches the end of a string. For example, xyz$ matches a string that ends with 'xyz'.
>
>     \ ：Used to escape special characters to treat them as ordinary characters. For example, \\. matches the period character '.'.
>
>     \s ：Matches any whitespace character, including spaces, tabs, newline characters, etc.
>
>     \d ：Matches any digit character, equivalent to [0-9].
>
>     \w ：Matches any word character, including letters, digits, and underscores, equivalent to [a-zA-Z0-9_].

### Keywords

````text
CREATE, SQL_BLCOK_RULE
````

### Best Practice

