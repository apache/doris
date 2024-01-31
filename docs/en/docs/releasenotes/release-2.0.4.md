---
{
    "title": "Release 2.0.4",
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


Thanks to our community users and developers, about 333 improvements and bug fixes have been made in Doris 2.0.4 version.

**Quick Download** : [https://doris.apache.org/download/](https://doris.apache.org/download/)

**GitHub** : [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## Behavior change
- More reasonable and accurate precision and scale inference for decimal data type
  - [https://github.com/apache/doris/pull/28034](https://github.com/apache/doris/pull/28034)

- Support drop policy for user or role
  - [https://github.com/apache/doris/pull/29488](https://github.com/apache/doris/pull/29488)

## New features

- Support datev1, datetimev1 and decimalv2 datatypes in new optimizer Nereids.
- Support ODBC table for new optimizer Nereids.
- Add `lower_case` and `ignore_above` option for inverted index
- Support `match_regexp` and `match_phrase_prefix` optimization by inverted index
- Support paimon native reader in datalake
- Support audit-log for `insert into` SQL
- Support reading parquet file in lzo compressed format

## Three Improvement and optimizations

- Improve storage management including balance, migration, publish and others.
- Improve storage cooldown policy to use save disk space.
- Performance optimization for substr with ascii string.
- Improve partition prune when date function is used.
- Improve auto analyze visibility and performance.

See the complete list of improvements and bug fixes on github [dev/2.0.4-merged](https://github.com/apache/doris/issues?q=label%3Adev%2F2.0.4-merged+is%3Aclosed)



## Credits
Last but not least, this release would not have been possible without the following contributors: 

airborne12, amorynan, AshinGau, BePPPower, bingquanzhao, BiteTheDDDDt, bobhan1, ByteYue, caiconghui,CalvinKirs, cambyzju, caoliang-web, catpineapple, csun5285, dataroaring, deardeng, dutyu, eldenmoon, englefly, feifeifeimoon, fornaix, Gabriel39, gnehil, HappenLee, hello-stephen, HHoflittlefish777,hubgeter, hust-hhb, ixzc, jacktengg, jackwener, Jibing-Li, kaka11chen, KassieZ, LemonLiTree,liaoxin01, LiBinfeng-01, lihuigang, liugddx, luwei16, morningman, morrySnow, mrhhsg, Mryange, nextdreamblue, Nitin-Kashyap, platoneko, py023, qidaye, shuke987, starocean999, SWJTU-ZhangLei, w41ter, wangbo, wsjz, wuwenchi, Xiaoccer, xiaokang, XieJiann, xingyingone, xinyiZzz, xuwei0912, xy720, xzj7019, yujun777, zclllyybb, zddr, zhangguoqiang666, zhangstar333, zhannngchen, zhiqiang-hhhh, zy-kkk, zzzxl1993