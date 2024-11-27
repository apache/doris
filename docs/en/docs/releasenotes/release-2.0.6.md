---
{
    "title": "Release 2.0.6",
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



Thanks to our community users and developers, about 114 improvements and bug fixes have been created by 51 contributors in Doris 2.0.6 version.

**Quick Download：** [https://doris.apache.org/download/](https://doris.apache.org/download/)

**GitHub：** [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## Behavior change
- N/A

## New features
- Support match a function with alias in materialized-view
- Add a command to drop a tablet replica safely on backend
- Add row count cache for external table.
- Support analyze rollup to gather statistics for optimizer

## Improvement and optimizations
- Improve tablet schema cache memory by using deterministic way to serialize protobuf
- Improve show column stats performance
- Support estimate row count for iceberg and paimon
- Support sqlserver timestamp type read for JDBC catalog


See the complete list of improvements and bug fixes on [github](https://github.com/apache/doris/compare/2.0.5-rc02...2.0.6).


## Credits
Thanks all who contribute to this release:

924060929, AshinGau, BePPPower, BiteTheDDDDt, CalvinKirs, cambyzju, deardeng, DongLiang-0, eldenmoon, englefly, feelshana, feiniaofeiafei, felixwluo, HappenLee, hust-hhb, iwanttobepowerful, ixzc, JackDrogon, Jibing-Li, KassieZ, larshelge, liaoxin01, LiBinfeng-01, liutang123, luennng, morningman, morrySnow, mrhhsg, qidaye, starocean999, TangSiyang2001, wangbo, wsjz, wuwenchi, xiaokang, XieJiann, xuwei0912, xy720, xzj7019, yiguolei, yujun777, Yukang-Lian, Yulei-Yang, zclllyybb, zddr, zhangstar333, zhannngchen, zhiqiang-hhhh, zy-kkk, zzzxl1993

