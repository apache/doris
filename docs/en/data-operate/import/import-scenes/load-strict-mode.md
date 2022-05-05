---
{
    "title": "import strict mode",
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

# import strict mode

Strict mode (strict_mode) is configured as a parameter in the import operation. This parameter affects the import behavior of certain values and the final imported data.

This document mainly explains how to set strict mode, and the impact of strict mode.

## How to set

Strict mode is all False by default, i.e. off.

Different import methods set strict mode in different ways.

1. [BROKER LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.html)

   ```sql
   LOAD LABEL example_db.label1
   (
       DATA INFILE("bos://my_bucket/input/file.txt")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY ","
   )
   WITH BROKER bos
   (
       "bos_endpoint" = "http://bj.bcebos.com",
       "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxxx",
       "bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyyyyyy"
   )
   PROPERTIES
   (
       "strict_mode" = "true"
   )
   ````

2. [STREAM LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.html)

   ```bash
   curl --location-trusted -u user:passwd \
   -H "strict_mode: true" \
   -T 1.txt \
   http://host:port/api/example_db/my_table/_stream_load
   ````

3. [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.html)

   ```sql
   CREATE ROUTINE LOAD example_db.test_job ON my_table
   PROPERTIES
   (
       "strict_mode" = "true"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic"
   );
   ````

4. [INSERT](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.html)

   Set via [session variables](../../../advanced/variables.html):

   ```sql
   SET enable_insert_strict = true;
   INSERT INTO my_table ...;
   ````

## The role of strict mode

Strict mode means strict filtering of column type conversions during import.

The strict filtering strategy is as follows:

For column type conversion, if strict mode is turned on, the wrong data will be filtered. The wrong data here refers to: the original data is not `null`, but the result is `null` after column type conversion.

The `column type conversion` mentioned here does not include the `null` value calculated by the function.

For an imported column type that contains range restrictions, if the original data can pass the type conversion normally, but cannot pass the range restrictions, strict mode will not affect it. For example: if the type is `decimal(1,0)` and the original data is 10, it belongs to the range that can be converted by type but is not within the scope of the column declaration. This kind of data strict has no effect on it.

1. Take the column type as TinyInt as an example:

   | Primitive data type | Primitive data example | Converted value to TinyInt | Strict mode | Result                   |
   | ------------------- | ---------------------- | -------------------------- | ----------- | ------------------------ |
   | NULL                | \N                     | NULL                       | ON or OFF   | NULL                     |
   | Non-null value      | "abc" or 2000          | NULL                       | On          | Illegal value (filtered) |
   | non-null value      | "abc"                  | NULL                       | off         | NULL                     |
   | non-null value      | 1                      | 1                          | on or off   | import correctly         |

   > Description:
   >
   > 1. Columns in the table allow to import null values
   > 2. After `abc` and `2000` are converted to TinyInt, they will become NULL due to type or precision issues. When strict mode is on, this data will be filtered. And if it is closed, `null` will be imported.

2. Take the column type as Decimal(1,0) as an example

   | Primitive Data Types | Examples of Primitive Data | Converted to Decimal | Strict Mode | Result                   |
   | -------------------- | -------------------------- | -------------------- | ----------- | ------------------------ |
   | Null                 | \N                         | null                 | On or Off   | NULL                     |
   | non-null value       | aaa                        | NULL                 | on          | illegal value (filtered) |
   | non-null value       | aaa                        | NULL                 | off         | NULL                     |
   | non-null value       | 1 or 10                    | 1 or 10              | on or off   | import correctly         |

   > Description:
   >
   > 1. Columns in the table allow to import null values
   > 2. After `abc` is converted to Decimal, it will become NULL due to type problem. When strict mode is on, this data will be filtered. And if it is closed, `null` will be imported.
   > 3. Although `10` is an out-of-range value, because its type conforms to the requirements of decimal, strict mode does not affect it. `10` will eventually be filtered in other import processing flows. But not filtered by strict mode.
