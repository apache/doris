---
{
    "title": "Data Transformation, Column Mapping and Filtering",
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

# Imported Data Transformation, Column Mapping and Filtering

## Supported import methods

- [BROKER LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md)

  ```sql
  LOAD LABEL example_db.label1
  (
      DATA INFILE("bos://bucket/input/file")
      INTO TABLE `my_table`
      (k1, k2, tmpk3)
      PRECEDING FILTER k1 = 1
      SET (
          k3 = tmpk3 + 1
      )
      WHERE k1 > k2
  )
  WITH BROKER bos
  (
      ...
  );
  ````

- [STREAM LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md)

  ```bash
  curl
  --location-trusted
  -u user:passwd
  -H "columns: k1, k2, tmpk3, k3 = tmpk3 + 1"
  -H "where: k1 > k2"
  -T file.txt
  http://host:port/api/testDb/testTbl/_stream_load
  ````

- [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md)

  ```sql
  CREATE ROUTINE LOAD example_db.label1 ON my_table
  COLUMNS(k1, k2, tmpk3, k3 = tmpk3 + 1),
  PRECEDING FILTER k1 = 1,
  WHERE k1 > k2
  ...
  ````

The above import methods all support column mapping, transformation and filtering operations on the source data:

- Pre-filtering: filter the read raw data once.

  ````text
  PRECEDING FILTER k1 = 1
  ````

- Mapping: Define the columns in the source data. If the defined column name is the same as the column in the table, it is directly mapped to the column in the table. If different, the defined column can be used for subsequent transformation operations. As in the example above:

  ````text
  (k1, k2, tmpk3)
  ````

- Conversion: Convert the mapped columns in the first step, you can use built-in expressions, functions, and custom functions for conversion, and remap them to the corresponding columns in the table. As in the example above:

  ````text
  k3 = tmpk3 + 1
  ````

- Post filtering: Filter the mapped and transformed columns by expressions. Filtered data rows are not imported into the system. As in the example above:

  ````text
  WHERE k1 > k2
  ````

## column mapping

The purpose of column mapping is mainly to describe the information of each column in the import file, which is equivalent to defining the name of the column in the source data. By describing the column mapping relationship, we can import source files with different column order and different number of columns into Doris. Below we illustrate with an example:

Assuming that the source file has 4 columns, the contents are as follows (the header column names are only for convenience, and there is no header actually):

| Column 1 | Column 2 | Column 3  | Column 4 |
| -------- | -------- | --------- | -------- |
| 1        | 100      | beijing   | 1.1      |
| 2        | 200      | shanghai  | 1.2      |
| 3        | 300      | guangzhou | 1.3      |
| 4        | \N       | chongqing | 1.4      |

> Note: `\N` means null in the source file.

1. Adjust the mapping order

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. The import mapping relationship we want is as follows:

   ````text
   column 1 -> k1
   column 2 -> k3
   column 3 -> k2
   column 4 -> k4
   ````

   Then the column mapping should be written in the following order:

   ````text
   (k1, k3, k2, k4)
   ````

2. There are more columns in the source file than in the table

   Suppose there are 3 columns `k1,k2,k3` in the table. The import mapping relationship we want is as follows:

   ````text
   column 1 -> k1
   column 2 -> k3
   column 3 -> k2
   ````

   Then the column mapping should be written in the following order:

   ````text
   (k1, k3, k2, tmpk4)
   ````

   where `tmpk4` is a custom column name that does not exist in the table. Doris ignores this non-existing column name.

3. The number of columns in the source file is less than the number of columns in the table, fill with default values

   Suppose there are 5 columns `k1,k2,k3,k4,k5` in the table. The import mapping relationship we want is as follows:

   ````text
   column 1 -> k1
   column 2 -> k3
   column 3 -> k2
   ````

   Here we only use the first 3 columns from the source file. The two columns `k4,k5` want to be filled with default values.

   Then the column mapping should be written in the following order:

   ````text
   (k1, k3, k2)
   ````

   If the `k4,k5` columns have default values, the default values will be populated. Otherwise, if it is a `nullable` column, it will be populated with a `null` value. Otherwise, the import job will report an error.

## Column pre-filtering

Pre-filtering is to filter the read raw data once. Currently only BROKER LOAD and ROUTINE LOAD are supported.

Pre-filtering has the following application scenarios:

1. Filter before conversion

   Scenarios where you want to filter before column mapping and transformation. It can filter out some unwanted data first.

2. The filter column does not exist in the table, it is only used as a filter identifier

   For example, the source data stores the data of multiple tables (or the data of multiple tables is written to the same Kafka message queue). Each row in the data has a column name to identify which table the row of data belongs to. Users can filter the corresponding table data for import by pre-filtering conditions.

## Column conversion

The column transformation function allows users to transform column values in the source file. Currently, Doris supports most of the built-in functions and user-defined functions for conversion.

> Note: The user-defined function belongs to a certain database. When using the user-defined function for conversion, the user needs to have read permission to this database.

Transformation operations are usually defined along with column mappings. That is, the columns are first mapped and then converted. Below we illustrate with an example:

Assuming that the source file has 4 columns, the contents are as follows (the header column names are only for convenience, and there is no header actually):

| Column 1 | Column 2 | Column 3  | Column 4 |
| -------- | -------- | --------- | -------- |
| 1        | 100      | beijing   | 1.1      |
| 2        | 200      | shanghai  | 1.2      |
| 3        | 300      | guangzhou | 1.3      |
| \N       | 400      | chongqing | 1.4      |

1. Convert the column values in the source file and import them into the table

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. Our desired import mapping and transformation relationship is as follows:

   ````text
   column 1 -> k1
   column 2 * 100 -> k3
   column 3 -> k2
   column 4 -> k4
   ````

   Then the column mapping should be written in the following order:

   ````text
   (k1, tmpk3, k2, k4, k3 = tmpk3 * 100)
   ````

   This is equivalent to us naming the second column in the source file `tmpk3`, and specifying that the value of the `k3` column in the table is `tmpk3 * 100`. The data in the final table is as follows:

   | k1   | k2        | k3    | k4   |
   | ---- | --------- | ----- | ---- |
   | 1    | beijing   | 10000 | 1.1  |
   | 2    | shanghai  | 20000 | 1.2  |
   | 3    | guangzhou | 30000 | 1.3  |
   | null | chongqing | 40000 | 1.4  |

2. Through the case when function, column conversion is performed conditionally.

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. We hope that `beijing, shanghai, guangzhou, chongqing` in the source data are converted to the corresponding region ids and imported:

   ````text
   column 1 -> k1
   column 2 -> k2
   Column 3 after region id conversion -> k3
   column 4 -> k4
   ````

   Then the column mapping should be written in the following order:

   ````text
   (k1, k2, tmpk3, k4, k3 = case tmpk3 when "beijing" then 1 when "shanghai" then 2 when "guangzhou" then 3 when "chongqing" then 4 else null end)
   ````

   The data in the final table is as follows:

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 1    | 100  | 1    | 1.1  |
   | 2    | 200  | 2    | 1.2  |
   | 3    | 300  | 3    | 1.3  |
   | null | 400  | 4    | 1.4  |

3. Convert the null value in the source file to 0 and import it. At the same time, the region id conversion in Example 2 is also performed.

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. While converting the region id, we also want to convert the null value of the k1 column in the source data to 0 and import:

   ````text
   Column 1 is converted to 0 if it is null -> k1
   column 2 -> k2
   column 3 -> k3
   column 4 -> k4
   ````

   Then the column mapping should be written in the following order:

   ````text
   (tmpk1, k2, tmpk3, k4, k1 = ifnull(tmpk1, 0), k3 = case tmpk3 when "beijing" then 1 when "shanghai" then 2 when "guangzhou" then 3 when "chongqing" then 4 else null end)
   ````

   The data in the final table is as follows:

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 1    | 100  | 1    | 1.1  |
   | 2    | 200  | 2    | 1.2  |
   | 3    | 300  | 3    | 1.3  |
   | 0    | 400  | 4    | 1.4  |

## List filter

After column mapping and transformation, we can filter the data that we do not want to import into Doris through filter conditions. Below we illustrate with an example:

Assuming that the source file has 4 columns, the contents are as follows (the header column names are only for convenience, and there is no header actually):

| Column 1 | Column 2 | Column 3  | Column 4 |
| -------- | -------- | --------- | -------- |
| 1        | 100      | beijing   | 1.1      |
| 2        | 200      | shanghai  | 1.2      |
| 3        | 300      | guangzhou | 1.3      |
| \N       | 400      | chongqing | 1.4      |

1. In the default case of column mapping and transformation, filter directly

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. We can define filter conditions directly with default column mapping and transformation. If we want to import only the data rows whose fourth column in the source file is greater than 1.2, the filter conditions are as follows:

   ````text
   where k4 > 1.2
   ````

   The data in the final table is as follows:

   | k1   | k2   | k3        | k4   |
   | ---- | ---- | --------- | ---- |
   | 3    | 300  | guangzhou | 1.3  |
   | null | 400  | chongqing | 1.4  |

   By default, Doris maps columns sequentially, so column 4 in the source file is automatically mapped to column `k4` in the table.

2. Filter the column-transformed data

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. In the **column conversion** example, we converted province names to ids. Here we want to filter out the data with id 3. Then the conversion and filter conditions are as follows:

   ````text
   (k1, k2, tmpk3, k4, k3 = case tmpk3 when "beijing" then 1 when "shanghai" then 2 when "guangzhou" then 3 when "chongqing" then 4 else null end)
   where k3 != 3
   ````

   The data in the final table is as follows:

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 1    | 100  | 1    | 1.1  |
   | 2    | 200  | 2    | 1.2  |
   | null | 400  | 4    | 1.4  |

   Here we see that the column value when performing the filter is the final column value after mapping and transformation, not the original data.

3. Multi-condition filtering

   Suppose there are 4 columns `k1,k2,k3,k4` in the table. We want to filter out the data whose `k1` column is `null`, and at the same time filter out the data whose `k4` column is less than 1.2, the filter conditions are as follows:

   ````text
   where k1 is not null and k4 >= 1.2
   ````

   The data in the final table is as follows:

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 2    | 200  | 2    | 1.2  |
   | 3    | 300  | 3    | 1.3  |

### Data Quality Issues and Filtering Thresholds

The rows of data processed in an import job can be divided into the following three types:

1. Filtered Rows

   Data that was filtered out due to poor data quality. Unqualified data quality includes data format problems such as type error, precision error, long string length, mismatched file column number, and data rows that are filtered out because there is no corresponding partition.

2. Unselected Rows

   This part is the row of data that was filtered out due to `preceding filter` or `where` column filter conditions.

3. Loaded Rows

   Rows of data being imported correctly.

Doris's import task allows the user to set a maximum error rate (`max_filter_ratio`). If the error rate of the imported data is below the threshold, those erroneous rows will be ignored and other correct data will be imported.

The error rate is calculated as:

````text
#Filtered Rows / (#Filtered Rows + #Loaded Rows)
````

That is to say, `Unselected Rows` will not participate in the calculation of the error rate.
