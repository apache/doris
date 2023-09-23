---
{
"title": "DBT Doris Adapter",
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

# DBT Doris Adapter

[DBT(Data Build Tool)](https://docs.getdbt.com/docs/introduction) is a component that focuses on doing T (Transform) in ELT (extraction, loading, transformation) - the "transformation data" link
The `dbt-doris` adapter is developed based on `dbt-core` 1.5.0 and relies on the `mysql-connector-python` driver to convert data to doris.

git：https://github.com/apache/doris/tree/master/extension/dbt-doris

## version

| doris   | python       | dbt-core |
|---------|--------------|----------|
| >=1.2.5 | >=3.8,<=3.10 | >=1.5.0  |


## dbt-doris adapter Instructions

### dbt-doris adapter install
use pip install：
```shell
pip install dbt-doris
```
check version：
```shell
dbt --version
```
if command not found: dbt：
```shell
ln -s /usr/local/python3/bin/dbt /usr/bin/dbt
```

### dbt-doris adapter project init
```shell
dbt init 
```
Users need to prepare the following information to init dbt project

| name     |  default | meaning                                                                                                                                   |  
|----------|------|-------------------------------------------------------------------------------------------------------------------------------------------|
| project  |      | project name                                                                                                                              | 
| database |      | Enter the corresponding number to select the adapter （选择doris）                                                                            | 
| host     |      | doris host                                                                                                                                | 
| port     | 9030 | doris MySQL Protocol Port                                                                                                                 |
| schema   |      | In dbt-doris, it is equivalent to database, Database name                                                                                 |
| username |      | doris username                                                                                                                            |
| password |      | doris password                                                                                                                            |
| threads  | 1    | Parallelism in dbt-doris (setting a parallelism that does not match the cluster capability will increase the risk of dbt running failure) |


### dbt-doris adapter run
For dbt run documentation, please refer to [here](https://docs.getdbt.com/docs/get-started/run-your-dbt-projects).
Go to the project directory and execute the default dbt model:
```shell
dbt run 
```
model：`my_first_dbt_model`和`my_second_dbt_model`

They are materialized `table` and `view` respectively.
then login to doris to view the data results and table creation statements of `my_first_dbt_model` and `my_second_dbt_model`.
### dbt-doris adapter Materialization
dbt-doris Materialization support three:
1. view
2. table
3. incremental

#### View 

Using `view` as the materialization, Models will be rebuilt as views each time they are run through the create view as statement. (By default, the materialization method of dbt is view)
``` 
Advantages: No extra data is stored, and views on top of the source data will always contain the latest records.
Disadvantages: View queries that perform large transformations or are nested on top of other views are slow.
Recommendation: Usually start with the view of the model and only change to another materialization if there are performance issues. Views are best suited for models that do not undergo major transformations, such as renaming, column changes.
```

config：
```yaml
models:
  <resource-path>:
    +materialized: view
```
Or write in the model file
```jinja
{{ config(materialized = "view") }}
```

#### Table

When using the `table` materialization mode, your model is rebuilt as a table at each run with a `create table as select` statement.
For the tablet materialization of dbt, dbt-doris uses the following steps to ensure the atomicity of data changes:
1. first create a temporary table: `create table this_table_temp as {{ model sql}}`.
2. Determine whether `this_table` does not exist, that is, it is created for the first time, execute `rename`, and change the temporary table to the final table.
3. if already exists, then `alter table this_table REPLACE WITH TABLE this_table_temp PROPERTIES('swap' = 'False')`，This operation can exchange the table name and delete the `this_table_temp` temporary table，[this](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-REPLACE.md) guarantees the atomicity of this operation through the transaction mechanism of the Doris.

``` 
Advantages: table query speed will be faster than view.
Disadvantages: The table takes a long time to build or rebuild, additional data will be stored, and incremental data synchronization cannot be performed.
Recommendation: It is recommended to use the table materialization method for models queried by BI tools or models with slow operations such as downstream queries and conversions.
```

config:
```yaml
models:
  <resource-path>:
    +materialized: table
    +duplicate_key: [ <column-name>, ... ],
    +replication_num: int,
    +partition_by: [ <column-name>, ... ],
    +partition_type: <engine-type>,
    +partition_by_init: [<pertition-init>, ... ]
    +distributed_by: [ <column-name>, ... ],
    +buckets: int | 'auto',
    +properties: {<key>:<value>,...}
```
Or write in the model file:
```jinja
{{ config(
    materialized = "table",
    duplicate_key = [ "<column-name>", ... ],
    replication_num = "<int>"
    partition_by = [ "<column-name>", ... ],
    partition_type = "<engine-type>",
    partition_by_init = ["<pertition-init>", ... ]
    distributed_by = [ "<column-name>", ... ],
    buckets = "<int>" | "auto",
    properties = {"<key>":"<value>",...}
      ...
    ]
) }}
```

The details of the above configuration items are as follows:

| item                 | description                                                | Required? |
|---------------------|------------------------------------------------------------|-----------|
| `materialized`      | The materialized form of the table (Doris Duplicate table) | Required  |
| `duplicate_key`     | Doris Duplicate key                                        | Optional  |
| `replication_num`   | Number of table replicas                                   | Optional  |
| `partition_by`      | Table partition column                                     | Optional  |
| `partition_type`    | Table partition type, `range` or `list`.(default: `RANGE`) | Optional  |
| `partition_by_init` | Initialized table partitions                               | Optional  |
| `distributed_by`    | Table distributed column                                   | Optional  |
| `buckets`           | Bucket size                                                | Optional  |
| `properties`        | Doris table properties                                     | Optional  |




#### Incremental

Based on the incremental model results of the last run of dbt, records are incrementally inserted or updated into the table.
There are two ways to realize the increment of doris. `incremental_strategy` has two incremental strategies:
* `insert_overwrite`: Depends on the doris `unique` model. If there is an incremental requirement, specify the materialization as incremental when initializing the data of the model, and aggregate by specifying the aggregation column to achieve incremental data coverage.
* `append`: Depends on the doris `duplicate` model, it only appends incremental data and does not involve modifying any historical data. So no need to specify unique_key.
``` 
Advantages: Significantly reduces build time by only converting new records.
Disadvantages: incremental mode requires additional configuration, which is an advanced usage of dbt, and requires the support of complex scenarios and the adaptation of corresponding components.
Recommendation: The incremental model is best for event-based scenarios or when dbt runs become too slow
```

config:
```yaml
models:
  <resource-path>:
    +materialized: incremental
    +incremental_strategy: <strategy>
    +unique_key: [ <column-name>, ... ],
    +replication_num: int,
    +partition_by: [ <column-name>, ... ],
    +partition_type: <engine-type>,
    +partition_by_init: [<pertition-init>, ... ]
    +distributed_by: [ <column-name>, ... ],
    +buckets: int | 'auto',
    +properties: {<key>:<value>,...}
```
Or write in the model file:
```jinja
{{ config(
    materialized = "incremental",
    incremental_strategy = "<strategy>"
    unique_key = [ "<column-name>", ... ],
    replication_num = "<int>"
    partition_by = [ "<column-name>", ... ],
    partition_type = "<engine-type>",
    partition_by_init = ["<pertition-init>", ... ]
    distributed_by = [ "<column-name>", ... ],
    buckets = "<int>" | "auto",
    properties = {"<key>":"<value>",...}
      ...
    ]
) }}
```

The details of the above configuration items are as follows:

| item                 | description                                                       | Required? |
|----------------------------|-------------------------------------------------------------------|-----------|
| `materialized`             | The materialized form of the table (Doris Duplicate/Unique table) | Required  |
| `incremental_strategy`     | Incremental_strategy                                              | Optional  |
| `unique_key`               | Doris Unique key                                                  | Optional  |
| `replication_num`          | Number of table replicas                                          | Optional  |
| `partition_by`             | Table partition column                                            | Optional  |
| `partition_type`           | Table partition type, `range` or `list`.(default: `RANGE`)        | Optional  |
| `partition_by_init`        | Initialized table partitions                                      | Optional  |
| `distributed_by`           | Table distributed column                                          | Optional  |
| `buckets`                  | Bucket size                                                       | Optional  |
| `properties`               | Doris table properties                                            | Optional  |



### dbt-doris adapter seed

[`seed`](https://docs.getdbt.com/faqs/seeds/build-one-seed) is a functional module used to load data files such as csv. It is a way to load files into the library and participate in model building, but there are the following precautions:
1. Seeds should not be used to load raw data (for example, large CSV exports from a production database).
2. Since seeds are version controlled, they are best suited to files that contain business-specific logic, for example a list of country codes or user IDs of employees. 
3. Loading CSVs using dbt's seed functionality is not performant for large files. Consider using `streamload` to load these CSVs into doris.

Users can see the seeds directory under the dbt project directory, upload the csv file and seed configuration file in it and run
```shell
 dbt seed --select seed_name
```

Common seed configuration file writing method supports the definition of column types:
```yaml
seeds:
  seed_name: 
    config: 
      schema: demo_seed 
      full_refresh: true
      replication_num: 1
      column_types:
        id: bigint
        phone: varchar(32)
        ip: varchar(15)
        name: varchar(20)
        cost: DecimalV3(19,10)
```