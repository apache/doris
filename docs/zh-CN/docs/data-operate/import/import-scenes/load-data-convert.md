---
{
    "title": "导入的数据转换、列映射及过滤",
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

# 导入的数据转换、列映射及过滤

## 支持的导入方式

- [BROKER LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD)

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
  ```

- [STREAM LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md)

  ```bash
  curl
  --location-trusted
  -u user:passwd
  -H "columns: k1, k2, tmpk3, k3 = tmpk3 + 1"
  -H "where: k1 > k2"
  -T file.txt
  http://host:port/api/testDb/testTbl/_stream_load
  ```

- [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md)

  ```sql
  CREATE ROUTINE LOAD example_db.label1 ON my_table
  COLUMNS(k1, k2, tmpk3, k3 = tmpk3 + 1),
  PRECEDING FILTER k1 = 1,
  WHERE k1 > k2
  ...
  ```

以上导入方式都支持对源数据进行列映射、转换和过滤操作：

- 前置过滤：对读取到的原始数据进行一次过滤。

  ```text
  PRECEDING FILTER k1 = 1
  ```

- 映射：定义源数据中的列。如果定义的列名和表中的列相同，则直接映射为表中的列。如果不同，则这个被定义的列可以用于之后的转换操作。如上面示例中的：

  ```text
  (k1, k2, tmpk3)
  ```

- 转换：将第一步中经过映射的列进行转换，可以使用内置表达式、函数、自定义函数进行转化，并重新映射到表中对应的列上。如上面示例中的：

  ```text
  k3 = tmpk3 + 1
  ```

- 后置过滤：对经过映射和转换后的列，通过表达式进行过滤。被过滤的数据行不会导入到系统中。如上面示例中的：

  ```text
  WHERE k1 > k2
  ```

## 列映射

列映射的目的主要是描述导入文件中各个列的信息，相当于为源数据中的列定义名称。通过描述列映射关系，我们可以将于表中列顺序不同、列数量不同的源文件导入到 Doris 中。下面我们通过示例说明：

假设源文件有4列，内容如下（表头列名仅为方便表述，实际并无表头）：

| 列1  | 列2  | 列3       | 列4  |
| ---- | ---- | --------- | ---- |
| 1    | 100  | beijing   | 1.1  |
| 2    | 200  | shanghai  | 1.2  |
| 3    | 300  | guangzhou | 1.3  |
| 4    | \N   | chongqing | 1.4  |

> 注：`\N` 在源文件中表示 null。

1. 调整映射顺序

   假设表中有 `k1,k2,k3,k4` 4列。我们希望的导入映射关系如下：

   ```text
   列1 -> k1
   列2 -> k3
   列3 -> k2
   列4 -> k4
   ```

   则列映射的书写顺序应如下：

   ```text
   (k1, k3, k2, k4)
   ```

2. 源文件中的列数量多于表中的列

   假设表中有 `k1,k2,k3` 3列。我们希望的导入映射关系如下：

   ```text
   列1 -> k1
   列2 -> k3
   列3 -> k2
   ```

   则列映射的书写顺序应如下：

   ```text
   (k1, k3, k2, tmpk4)
   ```

   其中 `tmpk4` 为一个自定义的、表中不存在的列名。Doris 会忽略这个不存在的列名。

3. 源文件中的列数量少于表中的列，使用默认值填充

   假设表中有 `k1,k2,k3,k4,k5` 5列。我们希望的导入映射关系如下：

   ```text
   列1 -> k1
   列2 -> k3
   列3 -> k2
   ```

   这里我们仅使用源文件中的前3列。`k4,k5` 两列希望使用默认值填充。

   则列映射的书写顺序应如下：

   ```text
   (k1, k3, k2)
   ```

   如果 `k4,k5` 列有默认值，则会填充默认值。否则如果是 `nullable` 的列，则会填充 `null` 值。否则，导入作业会报错。

## 列前置过滤

前置过滤是对读取到的原始数据进行一次过滤。目前仅支持 BROKER LOAD 和 ROUTINE LOAD。

前置过滤有以下应用场景：

1. 转换前做过滤

   希望在列映射和转换前做过滤的场景。能够先行过滤掉部分不需要的数据。

2. 过滤列不存在于表中，仅作为过滤标识

   比如源数据中存储了多张表的数据（或者多张表的数据写入了同一个 Kafka 消息队列）。数据中每行有一列表名来标识该行数据属于哪个表。用户可以通过前置过滤条件来筛选对应的表数据进行导入。

## 列转换

列转换功能允许用户对源文件中列值进行变换。目前 Doris 支持使用绝大部分内置函数、用户自定义函数进行转换。

> 注：自定义函数隶属于某一数据库下，在使用自定义函数进行转换时，需要用户对这个数据库有读权限。

转换操作通常是和列映射一起定义的。即先对列进行映射，再进行转换。下面我们通过示例说明：

假设源文件有4列，内容如下（表头列名仅为方便表述，实际并无表头）：

| 列1  | 列2  | 列3       | 列4  |
| ---- | ---- | --------- | ---- |
| 1    | 100  | beijing   | 1.1  |
| 2    | 200  | shanghai  | 1.2  |
| 3    | 300  | guangzhou | 1.3  |
| \N   | 400  | chongqing | 1.4  |

1. 将源文件中的列值经转换后导入表中

   假设表中有 `k1,k2,k3,k4` 4列。我们希望的导入映射和转换关系如下：

   ```text
   列1       -> k1
   列2 * 100 -> k3
   列3       -> k2
   列4       -> k4
   ```

   则列映射的书写顺序应如下：

   ```text
   (k1, tmpk3, k2, k4, k3 = tmpk3 * 100)
   ```

   这里相当于我们将源文件中的第2列命名为 `tmpk3`，同时指定表中 `k3` 列的值为 `tmpk3 * 100`。最终表中的数据如下：

   | k1   | k2        | k3    | k4   |
   | ---- | --------- | ----- | ---- |
   | 1    | beijing   | 10000 | 1.1  |
   | 2    | shanghai  | 20000 | 1.2  |
   | 3    | guangzhou | 30000 | 1.3  |
   | null | chongqing | 40000 | 1.4  |

2. 通过 case when 函数，有条件的进行列转换。

   假设表中有 `k1,k2,k3,k4` 4列。我们希望对于源数据中的 `beijing, shanghai, guangzhou, chongqing` 分别转换为对应的地区id后导入：

   ```text
   列1                  -> k1
   列2                  -> k2
   列3 进行地区id转换后    -> k3
   列4                  -> k4
   ```

   则列映射的书写顺序应如下：

   ```text
   (k1, k2, tmpk3, k4, k3 = case tmpk3 when "beijing" then 1 when "shanghai" then 2 when "guangzhou" then 3 when "chongqing" then 4 else null end)
   ```

   最终表中的数据如下：

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 1    | 100  | 1    | 1.1  |
   | 2    | 200  | 2    | 1.2  |
   | 3    | 300  | 3    | 1.3  |
   | null | 400  | 4    | 1.4  |

3. 将源文件中的 null 值转换成 0 导入。同时也进行示例2中的地区id转换。

   假设表中有 `k1,k2,k3,k4` 4列。在对地区id转换的同时，我们也希望对于源数据中 k1 列的 null 值转换成 0 导入：

   ```text
   列1 如果为null 则转换成0   -> k1
   列2                      -> k2
   列3                      -> k3
   列4                      -> k4
   ```

   则列映射的书写顺序应如下：

   ```text
   (tmpk1, k2, tmpk3, k4, k1 = ifnull(tmpk1, 0), k3 = case tmpk3 when "beijing" then 1 when "shanghai" then 2 when "guangzhou" then 3 when "chongqing" then 4 else null end)
   ```

   最终表中的数据如下：

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 1    | 100  | 1    | 1.1  |
   | 2    | 200  | 2    | 1.2  |
   | 3    | 300  | 3    | 1.3  |
   | 0    | 400  | 4    | 1.4  |

## 列过滤

经过列映射和转换后，我们可以通过过滤条件将不希望导入到Doris中的数据进行过滤。下面我们通过示例说明：

假设源文件有4列，内容如下（表头列名仅为方便表述，实际并无表头）：

| 列1  | 列2  | 列3       | 列4  |
| ---- | ---- | --------- | ---- |
| 1    | 100  | beijing   | 1.1  |
| 2    | 200  | shanghai  | 1.2  |
| 3    | 300  | guangzhou | 1.3  |
| \N   | 400  | chongqing | 1.4  |

1. 在列映射和转换缺省的情况下，直接过滤

   假设表中有 `k1,k2,k3,k4` 4列。我们可以在缺省列映射和转换的情况下，直接定义过滤条件。如我们希望只导入源文件中第4列为大于 1.2 的数据行，则过滤条件如下：

   ```text
   where k4 > 1.2
   ```

   最终表中的数据如下：

   | k1   | k2   | k3        | k4   |
   | ---- | ---- | --------- | ---- |
   | 3    | 300  | guangzhou | 1.3  |
   | null | 400  | chongqing | 1.4  |

   缺省情况下，Doris 会按照顺序进行列映射，因此源文件中的第4列自动被映射到表中的 `k4` 列。

2. 对经过列转换的数据进行过滤

   假设表中有 `k1,k2,k3,k4` 4列。在 **列转换** 示例中，我们将省份名称转换成了id。这里我们想过滤掉 id 为 3 的数据。则转换、过滤条件如下：

   ```text
   (k1, k2, tmpk3, k4, k3 = case tmpk3 when "beijing" then 1 when "shanghai" then 2 when "guangzhou" then 3 when "chongqing" then 4 else null end)
   where k3 != 3
   ```

   最终表中的数据如下：

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 1    | 100  | 1    | 1.1  |
   | 2    | 200  | 2    | 1.2  |
   | null | 400  | 4    | 1.4  |

   这里我们看到，执行过滤时的列值，为经过映射和转换后的最终列值，而不是原始数据。

3. 多条件过滤

   假设表中有 `k1,k2,k3,k4` 4列。我们想过滤掉 `k1` 列为 `null` 的数据，同时过滤掉 `k4` 列小于 1.2 的数据，则过滤条件如下：

   ```text
   where k1 is not null and k4 >= 1.2
   ```

   最终表中的数据如下：

   | k1   | k2   | k3   | k4   |
   | ---- | ---- | ---- | ---- |
   | 2    | 200  | 2    | 1.2  |
   | 3    | 300  | 3    | 1.3  |

### 数据质量问题和过滤阈值

导入作业中被处理的数据行可以分为如下三种：

1. Filtered Rows

   因数据质量不合格而被过滤掉的数据。数据质量不合格包括类型错误、精度错误、字符串长度超长、文件列数不匹配等数据格式问题，以及因没有对应的分区而被过滤掉的数据行。

2. Unselected Rows

   这部分为因 `preceding filter` 或 `where` 列过滤条件而被过滤掉的数据行。

3. Loaded Rows

   被正确导入的数据行。

Doris 的导入任务允许用户设置最大错误率（`max_filter_ratio`）。如果导入的数据的错误率低于阈值，则这些错误行将被忽略，其他正确的数据将被导入。

错误率的计算方式为：

```text
#Filtered Rows / (#Filtered Rows + #Loaded Rows)
```

也就是说 `Unselected Rows` 不会参与错误率的计算。
