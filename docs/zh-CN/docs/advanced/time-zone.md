---
{
    "title": "时区",
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

# 时区

Doris 支持自定义时区设置

## 基本概念

Doris 内部存在以下两个时区相关参数：

- system_time_zone : 当服务器启动时，会根据机器设置时区自动设置，设置后不可修改。
- time_zone : 集群当前时区，可以修改。

## 具体操作

1. `show variables like '%time_zone%'`

   查看当前时区相关配置

2. `SET [global] time_zone = 'Asia/Shanghai'`

   该命令可以设置session级别的时区，如使用`global`关键字，则Doris FE会将参数持久化，之后对所有新session生效。

## 数据来源

时区数据包含时区名、对应时间偏移量、夏令时变化情况等。在 BE 所在机器上，其数据来源依次为：

1. `TZDIR` 命令返回的目录，如不支持该命令，则为 `/usr/share/zoneinfo` 目录
2. Doris BE 部署目录下生成的 `zoneinfo` 目录。产生自 Doris Repository 下的 `resource/zoneinfo.tar.gz`

按顺序查找以上数据源，如果找到则使用当前项。如果 BE 配置项 `use_doris_tzfile` 为 true，则跳过对第一项的查找。如均未找到，则 Doris BE 将启动失败，请重新正确构建 BE 或获取发行版。

## 时区的影响

### 1. 函数

包括`NOW()`或`CURTIME()`等时间函数显示的值，也包括`show load`, `show backends`中的时间值。

但不会影响 `create table` 中时间类型分区列的 less than 值，也不会影响存储为 `date/datetime` 类型的值的显示。

受时区影响的函数：

- `FROM_UNIXTIME`：给定一个 UTC 时间戳，返回指定时区的日期时间：如 `FROM_UNIXTIME(0)`， 返回 CST 时区：`1970-01-01 08:00:00`。
- `UNIX_TIMESTAMP`：给定一个指定时区日期时间，返回 UTC 时间戳：如 CST 时区 `UNIX_TIMESTAMP('1970-01-01 08:00:00')`，返回 `0`。
- `CURTIME`：返回指定时区时间。
- `NOW`：返指定地时区日期时间。
- `CONVERT_TZ`：将一个日期时间从一个指定时区转换到另一个指定时区。

### 2. 时间类型的值

对于`DATE`, `DATEV2`, `DATETIME`, `DATETIMEV2`类型，我们支持插入数据时对时区进行转换。

- 如果数据带有时区，如 "2020-12-12 12:12:12+08:00"，而当前 Doris `time_zone = +00:00`，则得到实际值 "2020-12-12 04:12:12"。
- 如果数据不带有时区，如 "2020-12-12 12:12:12"，则认为该时间为绝对时间，不发生任何转换。

### 3. 夏令时

夏令时的本质是具名时区的实际时间偏移量，在一定日期内发生改变。

例如，`America/Los_Angeles`时区包含一次夏令时调整，起止时间为约为每年3月至11月。即，三月份夏令时开始时，`America/Los_Angeles`实际时区偏移由`-08:00`变为`-07:00`，11月夏令时结束时，又从`-07:00`变为`-08:00`。
如果不希望开启夏令时，则应设定 `time_zone` 为 `-08:00` 而非 `America/Los_Angeles`。

## 使用方式

时区值可以使用多种格式给出，以下是 Doris 中完善支持的标准格式：

1. 标准具名时区格式，如 "Asia/Shanghai", "America/Los_Angeles"
2. 标准偏移格式，如 "+02:30", "-10:00"
3. 缩写时区格式，当前仅支持:
   1. "GMT", "UTC"，等同于 "+00:00" 时区
   2. "CST", 等同于 "Asia/Shanghai" 时区
4. 单字母Z，代表Zulu时区，等同于 "+00:00" 时区

注意：由于实现方式的不同，当前 Doris 存在部分其他格式在部分导入方式中得到了支持。**生产环境不应当依赖这些未列于此的格式，它们的行为随时可能发生变化**，请关注版本更新时的相关 changelog。

## 最佳实践

### 时区敏感数据

时区问题主要涉及三个影响因素：

1. session variable `time_zone` —— 集群时区
2. Stream Load、Broker Load 等导入时指定的 header `timezone` —— 导入时区
3. 时区类型字面量 "2023-12-12 08:00:00+08:00" 中的 "+08:00" —— 数据时区

我们可以做如下理解：

Doris 目前兼容各时区下的数据向 Doris 中进行导入。而由于 `DATETIME` 等各个时间类型本身不内含时区信息，因此 Doris 集群内的时间类型数据，可以分为两类：

1. 绝对时间
2. 特定时区下的时间

所谓绝对时间是指，它所关联的数据场景与时区无关。对于这类数据，在导入时应该不带有任何时区后缀，它们将被原样存储。对于这类时间，因为不关联到实际的时区，取其 `unix_timestamp` 等函数结果是无实际意义的。而集群 `time_zone` 的改变不会影响它的使用。

所谓“某个特定时区下”的时间。这个“特定时区”就是我们的 session variable `time_zone`。就最佳实践而言，该变量应当在数据导入前确定，**且不再更改**。此时 Doris 集群中的该类时间数据，其实际意义为：在 `time_zone` 时区下的时间。例如：

```sql
mysql> select @@time_zone;
+----------------+
| @@time_zone    |
+----------------+
| Asia/Hong_Kong |
+----------------+
1 row in set (0.12 sec)

mysql> insert into dtv23 values('2020-12-12 12:12:12+02:00'); --- 绝对时区为+02:00
Query OK, 1 row affected (0.27 sec)

mysql> select * from dtv23;
+-------------------------+
| k0                      |
+-------------------------+
| 2020-12-12 18:12:12.000 | --- 被转换为 Doris 集群时区 Asia/Hong_Kong，应当保持此语义。
+-------------------------+
1 row in set (0.19 sec)

mysql> set time_zone = 'America/Los_Angeles';
Query OK, 0 rows affected (0.15 sec)

mysql> select * from dtv23;
+-------------------------+
| k0                      |
+-------------------------+
| 2020-12-12 18:12:12.000 | --- 如果修改 time_zone，时间值不会随之改变，其意义发生紊乱。
+-------------------------+
1 row in set (0.18 sec)

mysql> insert into dtv23 values('2020-12-12 12:12:12+02:00');
Query OK, 1 row affected (0.17 sec)

mysql> select * from dtv23;
+-------------------------+
| k0                      |
+-------------------------+
| 2020-12-12 02:12:12.000 |
| 2020-12-12 18:12:12.000 |
+-------------------------+ --- 此时可以发现，数据已经发生错乱。
2 rows in set (0.19 sec)
```

综上所述，处理时区问题最佳的实践是：

1. 在使用前确认该集群所表征的时区并设置 `time_zone`，在此之后不再更改。
2. 在导入时设定 header `timezone` 同集群 `time_zone` 一致。
3. 对于绝对时间，导入时不带时区后缀；对于有时区的时间，导入时带具体时区后缀，导入后将被转化至 Doris `time_zone` 时区。

### 夏令时

夏令时的起讫时间来自于[当前时区数据源](#数据来源)，不一定与当年度时区所在地官方实际确认时间完全一致。该数据由 ICANN 进行维护。如果需要确保夏令时表现与当年度实际规定一致，请保证 Doris 所选择的数据源为最新的 ICANN 所公布时区数据，下载途径见于[拓展阅读](#拓展阅读)中。

## 拓展阅读

- 时区格式列表：[List of tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
- IANA 时区数据库：[IANA Time Zone Database](https://www.iana.org/time-zones)
- ICANN 时区数据库：[The tz-announce Archives](https://mm.icann.org/pipermail/tz-announce/)
