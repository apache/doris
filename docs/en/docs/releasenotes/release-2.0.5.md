---
{
    "title": "Release 2.0.5",
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



Thanks to our community users and developers, about 217 improvements and bug fixes have been made in Doris 2.0.5 version.

**Quick Download：** [https://doris.apache.org/download/](https://doris.apache.org/download/)

**GitHub：** [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## Behavior change
- Change char function behaviour: `select char(0) = '\0'` return true as MySQL
  - https://github.com/apache/doris/pull/30034
- Allow exporting empty data
  - https://github.com/apache/doris/pull/30703

## New features
- Eliminate left outer join with `is null` condition
- Add `show-tablets-belong` stmt for analyzing a batch of tablet-ids
- InferPredicates support In, such as `a = b & a in [1, 2] -> b in [1, 2]`
- Optimize plan when column stats are unavailable
- Optimize plan using rollup column stats
- Support analyze materialized view
- Support ShowProcessStmt Show all FE connection

## Improvement and optimizations
- Optimize query plan when column stats are unaviable
- Optimize query plan using rollup column stats
- Stop analyze quickly after user close auto analyze
- Catch load column stats exception, avoid print too much stack info to fe.out
- Select materialized view by specify the view name in SQL
- Change auto analyze max table width default value to 100
- Escape characters for columns in recovery predicate pushdown in JDBC Catalog
- Fix JDBC MYSQL Catalog `to_date` fun pushdown
- Optimize the close logic of JDBC client
- Optimize JDBC connection pool parameter settings
- Obtain hudi partition information through HMS's API
- Optimize routine load job error msg and memory
- Skip all backup/restore jobs if max allowd option is set to 0

See the complete list of improvements and bug fixes on [github](https://github.com/apache/doris/compare/2.0.4-rc06...2.0.5-rc02).


## Credits
Thanks all who contribute to this release:

airborne12, alexxing662, amorynan, AshinGau, BePPPower, bingquanzhao, BiteTheDDDDt, ByteYue, caiconghui, cambyzju, catpineapple, dataroaring, eldenmoon, Emor-nj, englefly, felixwluo, GoGoWen, HappenLee, hello-stephen, HHoflittlefish777, HowardQin, JackDrogon, jacktengg, jackwener, Jibing-Li, KassieZ, LemonLiTree, liaoxin01, liugddx, LuGuangming, morningman, morrySnow, mrhhsg, Mryange, mymeiyi, nextdreamblue, qidaye, ryanzryu, seawinde,starocean999, TangSiyang2001, vinlee19, w41ter, wangbo, wsjz, wuwenchi, xiaokang, XieJiann, xingyingone, xy720,xzj7019, yujun777, zclllyybb, zhangstar333, zhannngchen, zhiqiang-hhhh, zxealous, zy-kkk, zzzxl1993

