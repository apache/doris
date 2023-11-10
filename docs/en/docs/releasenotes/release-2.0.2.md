---
{
    "title": "Release 2.0.2",
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

# Release 2.0.2

Thanks to our community users and developers, 489 improvements and bug fixes have been made in Doris 2.0.2.

## Behavior Changes

- [Remove json -> operator convert to json_extract #24679](https://github.com/apache/doris/pull/24679)

  Remove json '->' operator since it is conflicted with lambda function syntax. It's a syntax sugar for function json_extract and can be replaced with the former.
- [Start the script to set metadata_failure_recovery #24308](https://github.com/apache/doris/pull/24308)

  Move metadata_failure_recovery from fe.conf to start_fe.sh argument to prevent being used unexpectedly.
- [Change ordinary type null value is \N,complex type null value is null #24207](https://github.com/apache/doris/pull/24207)
- [Optimize priority_ network matching logic for be #23795](https://github.com/apache/doris/pull/23795)
- [Fix cancel load failed because Job could not be cancelled… #17730](https://github.com/apache/doris/pull/17730)
  
  Allow cancel a retrying load job.

## Improvements

### Easier to use

- [Support custom lib dir to save custom libs #23887](https://github.com/apache/doris/pull/23887)
  
  Add a custom_lib dir to allow users place custom lib files and custom_lib will not be replaced.
- [Optimize priority_ network matching logic #23784](https://github.com/apache/doris/pull/23784) 

  Optimize priority_network logic to avoid error when this config is wrong or not configured.
- [Row policy support role #23022](https://github.com/apache/doris/pull/23022) 

  Support role based auth for row policy.

### New optimizer Nereids statistics collection improvement

- [Disable file cache while running analysis tasks. #23663](https://github.com/apache/doris/pull/23663)
- [Show column stats even when error occurred. #23703](https://github.com/apache/doris/pull/23703)
- [Support basic jdbc external table stats collection. #23965](https://github.com/apache/doris/pull/23965)
- [Skip unknown col stats check on __internal_scheam and information_schema #24625](https://github.com/apache/doris/pull/24625)

### Better support for JDBC, HDFS, Hive, MySQL, Max Compute, Multi-Catalog

- [Support hadoop viewfs. #24168](https://github.com/apache/doris/pull/24168)
- [Avoid calling checksum when replaying creating jdbc catalog and fix ranger issue #22369](https://github.com/apache/doris/pull/22369)
- [Optimize the JDBC Catalog connection error message #23868](https://github.com/apache/doris/pull/23868) 

  Improve property check and error message for JDBC catalog
- [Fix mc decimal type parse, fix wrong obj location #24242](https://github.com/apache/doris/pull/24242) 

  Fix some issues for Max Compute catalog
- [Support sql cache for hms catalog #23391](https://github.com/apache/doris/pull/23391) 

  SQL cache for Hive catalog
- [Merge hms partition events. #22869](https://github.com/apache/doris/pull/22869) 

  Improve performance for Hive metadata sync
- [Add metadata_name_ids for quickly get catlogs,db,table and add profiling table in order to Compatible with mysql #22702](https://github.com/apache/doris/pull/22702)

### Performance for inverted index query

- [Add bkd index query cache to improve perf #23952](https://github.com/apache/doris/pull/23952)
- [Improve performance for count on index other than match #24678](https://github.com/apache/doris/pull/24678)
- [Improve match performance without index #24751](https://github.com/apache/doris/pull/24751)
- [Optimize multiple terms conjunction query #23871](https://github.com/apache/doris/pull/23871) 
Improve performance of MATCH_ALL
- [Optimize unnecessary conversions #24389](https://github.com/apache/doris/pull/24389) 
Improve performance of MATCH

### Improve Array functions

- [[Fix old optimizer with some array literal functions #23630](https://github.com/apache/doris/pull/23630)
- [Improve array union support multi params #24327](https://github.com/apache/doris/pull/24327)
- [Improve explode func with array nested complex type #24455](https://github.com/apache/doris/pull/24455)

## Important Bug fixes

- [The parameter positions of timestamp diff function to sql are reversed #23601](https://github.com/apache/doris/pull/23601)
- [Fix old optimizer with some array literal functions #23630](https://github.com/apache/doris/pull/23630)
- [Fix query cache returns wrong result after deleting partitions. #23555](https://github.com/apache/doris/pull/23555)
- [Fix potential data loss when clone task's dst tablet is cooldown replica #17644](https://github.com/apache/doris/pull/17644)
- [Fix array map batch append data with right next_array_item_rowid #23779](https://github.com/apache/doris/pull/23779)
- [Fix or to in rule #23940](https://github.com/apache/doris/pull/23940)
- [Fix 'char' function's toSql implementation is wrong #23860](https://github.com/apache/doris/pull/23860)
- [Record wrong best plan properties #23973](https://github.com/apache/doris/pull/23973)
- [Make TVF's distribution spec always be RANDOM #24020](https://github.com/apache/doris/pull/24020)
- [External scan use STORAGE_ANY instead of ANY as distibution #24039](https://github.com/apache/doris/pull/24039)
- [Runtimefilter target is not SlotReference #23958](https://github.com/apache/doris/pull/23958)
- [mv in select materialized_view should disable show table #24104](https://github.com/apache/doris/pull/24104)
- [Fail over to remote file reader if local cache failed #24097](https://github.com/apache/doris/pull/24097)
- [Fix revoke role operation cause fe down #23852](https://github.com/apache/doris/pull/23852)
- [Handle status code correctly and add a new error code `ENTRY_NOT_FOUND` #24139](https://github.com/apache/doris/pull/24139)
- [Fix leaky abstraction and shield the status code `END_OF_FILE` from upper layers #24165](https://github.com/apache/doris/pull/24165)
- [Fix bug that Read garbled files caused be crash. #24164](https://github.com/apache/doris/pull/24164)
- [Fix be core when user sepcified empty `column_separator` using hdfs tvf #24369](https://github.com/apache/doris/pull/24369)
- [Fix need to restart BE after replacing the jar package in java-udf #24372](https://github.com/apache/doris/pull/24372)
- [Need to call 'set_version' in nested functions #24381](https://github.com/apache/doris/pull/24381)
- [windown_funnel compatibility issue with multi backends #24385](https://github.com/apache/doris/pull/24385)
- [correlated anti join shouldn't be translated to null aware anti join #24290](https://github.com/apache/doris/pull/24290)
- [Change ordinary type null value is \N,complex type null value is null #24207](https://github.com/apache/doris/pull/24207)
- [Fix analyze failed when there are thousands of partitions. #24521](https://github.com/apache/doris/pull/24521)
- [Do not use enum as the data type for JavaUdfDataType. #24460](https://github.com/apache/doris/pull/24460)
- [Fix multi window projection issue temporarily #24568](https://github.com/apache/doris/pull/24568)
- [Make metadata compatible with 2.0.3 #24610](https://github.com/apache/doris/pull/24610)
- [Select outfile column order is wrong #24595](https://github.com/apache/doris/pull/24595)
- [Incorrect result of semi/anti mark join #24616](https://github.com/apache/doris/pull/24616)
- [Fix broker read issue #24635](https://github.com/apache/doris/pull/24635)
- [Skip unknown col stats check on __internal_scheam and information_schema #24625](https://github.com/apache/doris/pull/24625)
- [Fixed bug when parsing multi-character delimiters. #24572](https://github.com/apache/doris/pull/24572)
- [Fix timezone parse when there is no tzfile #24578](https://github.com/apache/doris/pull/24578)
- [We need to issue an error when starting FE without setting the Java home environment #23943](https://github.com/apache/doris/pull/23943)
- [Enable_unique_key_partial_update should be forwarded to master #24697](https://github.com/apache/doris/pull/24697)
- [Fix paimon file catalog meta issue and replication num analysis issue #24681](https://github.com/apache/doris/pull/24681)
- [Add more log for ingest_binlog && Fix ingest_binlog not rewrite rowset_meta tablet_uid #24617](https://github.com/apache/doris/pull/24617)
- [Do not abort when a disk is broken #24692](https://github.com/apache/doris/pull/24692)
- [colocate join could not work well on full outer join #24700](https://github.com/apache/doris/pull/24700)
- [Optimize unnecessary conversions #24389](https://github.com/apache/doris/pull/24389)
- [Optimize the reading efficiency of nullable (string) columns. #24698](https://github.com/apache/doris/pull/24698)
- [Fix segment cache core when output rowset is nullptr #24778](https://github.com/apache/doris/pull/24778)
- [Fix duplicate key in schema change #24782](https://github.com/apache/doris/pull/24782)
- [Make metadata compatible for future version after 2.0.2 #24800](https://github.com/apache/doris/pull/24800)
- [Fix map/array deserialize string with quote pair #24808](https://github.com/apache/doris/pull/24808)
- [Failed on arm platform, with clang compiler and pch on, close #24633 #24636](https://github.com/apache/doris/pull/24636)
- [Table column order is changed if add a column and do truncate #24981](https://github.com/apache/doris/pull/24981)
- [Make parser mode coarse grained by default #24949](https://github.com/apache/doris/pull/24949)

See the complete list of improvements and bug fixes on [github](https://github.com/apache/doris/issues?q=label%3Adev%2F2.0.2-merged+is%3Aclosed) .

## Big Thanks

Thanks all who contribute to this release:

[@adonis0147](https://github.com/adonis0147) [@airborne12](https://github.com/airborne12) [@amorynan](https://github.com/amorynan) [@AshinGau](https://github.com/AshinGau) [@BePPPower](https://github.com/BePPPower) [@BiteTheDDDDt](https://github.com/BiteTheDDDDt) [@bobhan1](https://github.com/bobhan1) [@ByteYue](https://github.com/ByteYue) [@caiconghui](https://github.com/caiconghui) [@CalvinKirs](https://github.com/CalvinKirs) [@cambyzju](https://github.com/cambyzju) [@ChengDaqi2023](https://github.com/ChengDaqi2023) [@ChinaYiGuan](https://github.com/ChinaYiGuan) [@CodeCooker17](https://github.com/CodeCooker17) [@csun5285](https://github.com/csun5285) [@dataroaring](https://github.com/dataroaring) [@deadlinefen](https://github.com/deadlinefen) [@DongLiang-0](https://github.com/DongLiang-0) [@Doris-Extras](https://github.com/Doris-Extras) [@dutyu](https://github.com/dutyu) [@eldenmoon](https://github.com/eldenmoon) [@englefly](https://github.com/englefly) [@freemandealer](https://github.com/freemandealer) [@Gabriel39](https://github.com/Gabriel39) [@gnehil](https://github.com/gnehil) [@GoGoWen](https://github.com/GoGoWen) [@gohalo](https://github.com/gohalo) [@HappenLee](https://github.com/HappenLee) [@hello-stephen](https://github.com/hello-stephen) [@HHoflittlefish777](https://github.com/HHoflittlefish777) [@hubgeter](https://github.com/hubgeter) [@hust-hhb](https://github.com/hust-hhb) [@ixzc](https://github.com/ixzc) [@JackDrogon](https://github.com/JackDrogon) [@jacktengg](https://github.com/jacktengg) [@jackwener](https://github.com/jackwener) [@Jibing-Li](https://github.com/Jibing-Li) [@JNSimba](https://github.com/JNSimba) [@kaijchen](https://github.com/kaijchen) [@kaka11chen](https://github.com/kaka11chen) [@Kikyou1997](https://github.com/Kikyou1997) [@Lchangliang](https://github.com/Lchangliang) [@LemonLiTree](https://github.com/LemonLiTree) [@liaoxin01](https://github.com/liaoxin01) [@LiBinfeng-01](https://github.com/LiBinfeng-01) [@liugddx](https://github.com/liugddx) [@luwei16](https://github.com/luwei16) [@mongo360](https://github.com/mongo360) [@morningman](https://github.com/morningman) [@morrySnow](https://github.com/morrySnow) @mrhhsg @Mryange @mymeiyi @neuyilan @pingchunzhang @platoneko @qidaye @realize096 @RYH61 @shuke987 @sohardforaname @starocean999 @SWJTU-ZhangLei @TangSiyang2001 @Tech-Circle-48 @w41ter @wangbo @wsjz @wuwenchi @wyx123654 @xiaokang @XieJiann @xinyiZzz @XuJianxu @xutaoustc @xy720 @xyfsjq @xzj7019 @yiguolei @yujun777 @Yukang-Lian @Yulei-Yang @zclllyybb @zddr @zhangguoqiang666 @zhangstar333 @ZhangYu0123 @zhannngchen @zxealous @zy-kkk @zzzxl1993 @zzzzzzzs
