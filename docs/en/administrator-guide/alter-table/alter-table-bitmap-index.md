---
{
    "title": "Bitmap Index",
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

# Bitmap Index
Users can speed up queries by creating a bitmap index
This document focuses on how to create an index job, as well as some considerations and frequently asked questions when creating an index.

## Glossary
* bitmap index: a fast data structure that speeds up queries

## Basic Principles
Creating and dropping index is essentially a schema change job. For details, please refer to
[Schema Change](alter-table-schema-change.html).

## Syntax
There are two forms of index creation and modification related syntax, one is integrated with alter table statement, and the other is using separate
create/drop index syntax
1. Create Index

    Please refer to [CREATE INDEX](../../sql-reference/sql-statements/Data%20Definition/CREATE%20INDEX.html) 
    or [ALTER TABLE](../../sql-reference/sql-statements/Data%20Definition/ALTER%20TABLE.html),
    You can also specify a bitmap index when creating a table, Please refer to [CREATE TABLE](../../sql-reference/sql-statements/Data%20Definition/CREATE%20TABLE.html)

2. Show Index

    Please refer to [SHOW INDEX](../../sql-reference/sql-statements/Administration/SHOW%20INDEX.html)

3. Drop Index

    Please refer to [DROP INDEX](../../sql-reference/sql-statements/Data%20Definition/DROP%20INDEX.html) or [ALTER TABLE](../../sql-reference/sql-statements/Data%20Definition/ALTER%20TABLE.html)

## Create Job
Please refer to [Schema Change](alter-table-schema-change.html)
## View Job
Please refer to [Schema Change](alter-table-schema-change.html)

## Cancel Job
Please refer to [Schema Change](alter-table-schema-change.html)

## Notice
* Currently only index of bitmap type is supported.
* The bitmap index is only created on a single column.
* Bitmap indexes can be applied to all columns of the `Duplicate` data model and key columns of the `Aggregate` and `Uniq` models.
* The data types supported by bitmap indexes are as follows:
    * `TINYINT`
    * `SMALLINT`
    * `INT`
    * `UNSIGNEDINT`
    * `BIGINT`
    * `CHAR`
    * `VARCHAR`
    * `DATE`
    * `DATETIME`
    * `LARGEINT`
    * `DECIMAL`
    * `BOOL`
* The bitmap index takes effect only in segmentV2. The table's storage format will be converted to V2 automatically when creating index.
