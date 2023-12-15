// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_array_functions_in_ck_case", "p0") {

    sql "drop table if exists numbers;"
    sql "drop table if exists hits;"
    sql "create table if not exists numbers (number int) ENGINE=OLAP DISTRIBUTED BY HASH(number) BUCKETS 1 PROPERTIES('replication_num' = '1');"
    sql "insert into numbers values (NULL), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);"
    sql "CREATE TABLE hits ( " +
            "WatchID UInt64,  " +
            "JavaEnable UInt8,  " +
            "Title String,  " +
            "GoodEvent Int16,  " +
            "EventTime DateTime,  " +
            "EventDate Date,  " +
            "CounterID UInt32,  " +
            "ClientIP UInt32,  " +
            "ClientIP6 char(16),  " +
            "RegionID UInt32,  " +
            "UserID UInt64,  " +
            "CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array<UInt16>,  URLCategories Array<UInt16>,  URLRegions Array<UInt32>,  RefererRegions Array<UInt32>,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,  FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8,  UserAgentMajor UInt16,  UserAgentMinor char(2),  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,  SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8,  IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,  HitColor char(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array<UInt16>,  RemoteIP UInt32,  RemoteIP6 char(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage char(2),  BrowserCountry char(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16,  SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32,  DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32,  NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8,  SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64,  ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  GoalsReached Array<UInt32>,  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,  UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  `ParsedParams.Key1` Array<String>,  `ParsedParams.Key2` Array(String),  `ParsedParams.Key3` Array(String),  `ParsedParams.Key4` Array(String),  `ParsedParams.Key5` Array<String>,  `ParsedParams.ValueDouble` Array<Float64>,  IslandID char(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime);"

    // ============= array join =========
    qt_order_qt_sql "SELECT 'array-join';"
    order_qt_sql "SELECT array_join(['Hello', 'World']);"
    order_qt_sql "SELECT array_join(['Hello', 'World'], ', ');"
    order_qt_sql "SELECT array_join([]);"
    order_qt_sql "SELECT array_join(array_array_range(number)) FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_join(array_array_range(number), '') FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_join(array_array_range(number), ',') FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_join(array_array_range(number % 4))) FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_join([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null], ';');"
    order_qt_sql "SELECT array_join(arr, ';') FROM SELECT [1, 23, 456] AS arr);"
    order_qt_sql "SELECT array_join(arr, ';') FROM SELECT [Null, 1, Null, 23, Null, 456, Null] AS arr);"
    order_qt_sql "SELECT array_join(arr, '; ') FROM SELECT [cast('127.0.0.1' as ipv4), cast('1.0.0.1' as ipv4)] AS arr);"
    order_qt_sql "SELECT array_join(arr, '; ') FROM SELECT [cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)] AS arr);"

    //array_with_constant
    qt_order_qt_sql "SELECT 'array_with_constant';"
    order_qt_sql "SELECT array_with_constant(3, number) FROM numbers(10);"
    order_qt_sql "SELECT array_with_constant(number, 'Hello') FROM numbers(10);"
    order_qt_sql "SELECT array_with_constant(number % 3, number % 2 ? 'Hello' : NULL) FROM numbers(10);"
    order_qt_sql "SELECT array_with_constant(number, []) FROM numbers(10);"
    order_qt_sql "SELECT array_with_constant(2, 'qwerty'), array_with_constant(0, -1), array_with_constant(1, 1);"
    //  -- { serverError }
    test {
        order_qt_sql "SELECT array_with_constant(-231.37104, -138);"
        exception "ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Array size can not be negative in function:array_with_constant"
    }

    // ========= array_intersect ===========
    // with sort
    qt_order_qt_sql "SELECT 'array_intersect-array-sort';"
    sql "drop table if exists array_intersect;"
    sql "create table array_intersect (date Date, arr Array<UInt8>)  ENGINE=OLAP DISTRIBUTED BY HASH(number) BUCKETS 1 PROPERTIES('replication_num' = '1');"

    sql "insert into array_intersect values ('2019-01-01', [1,2,3]);"
    sql "insert into array_intersect values ('2019-01-01', [1,2]);"
    sql "insert into array_intersect values ('2019-01-01', [1]);"
    sql "insert into array_intersect values ('2019-01-01', []);"

    order_qt_sql "SELECT array_sort(array_intersect(arr, [1,2])) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect(arr, [])) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([], arr)) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([1,2], arr)) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([], [])) from array_intersect order by arr;"


    order_qt_sql "SELECT array_sort(array_intersect(arr, [1,2])) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect(arr, [])) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([], arr)) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([1,2], arr)) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from array_intersect order by arr;"
    order_qt_sql "SELECT array_sort(array_intersect([], [])) from array_intersect order by arr;"


    order_qt_sql "SELECT array_sort(array_intersect([-100], [156]));"
    order_qt_sql "SELECT array_sort(array_intersect([1], [257]));"

    order_qt_sql "SELECT array_sort(array_intersect(['a', 'b', 'c'], ['a', 'a']));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 1], [2, 2]));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 1], [1, 2]));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 1, 1], [3], [2, 2, 2]));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 2], [1, 2], [2]));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [1]));"
    order_qt_sql "SELECT array_sort(array_intersect([]));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 2, 3]));"
    order_qt_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [2, 2, 2]));"

    // ========== array-zip ==========
    // wrong case
    test {
        sql "SELECT array_zip();"
        exception "ERROR 1105 (HY000): errCode = 2, detailMessage = No matching function with signature: array_zip()."
    }

    test {
        sql "SELECT array_zip(['a', 'b', 'c'], ['d', 'e', 'f', 'd']);"
        exception "ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]execute failed, function array_zip's 2-th argument should have same offsets with first argument"
    }

    test {
        sql "SELECT array_zip();"
        exception "ERROR 1105 (HY000): errCode = 2, detailMessage = No matching function with signature: array_zip(VARCHAR(*), VARCHAR(*), VARCHAR(*))."
    }

    // ============= array element_at =========
    // ubsan test
    qt_order_qt_sql "SELECT 'array_element_at';"
    order_qt_sql "SELECT array(number)[10000000000] FROM numbers;"
    order_qt_sql " SELECT array(number)[-10000000000] FROM numbers;"
    order_qt_sql " SELECT array(number)[-0x8000000000000000] FROM numbers;"
    order_qt_sql " SELECT array(number)[0xFFFFFFFFFFFFFFFF] FROM numbers;"
    order_qt_sql " SELECT array(number)[18446744073709551615] FROM numbers;"


    // predict not support action
    test {
        sql "SELECT array_range(100) == array_range(0, 100) and  array_range(0, 100) == array_range(0, 100, 1);"
        exception("ERROR 1105 (HY000): errCode = 2, detailMessage = comparison predicate could not contains complex type: (array_range(100) = array_range(0, 100))")
    }

    order_qt_sql "SELECT distinct size(array_range(number, number + 100, 99))=2 FROM numbers;"
    order_qt_sql "SELECT array_map(x -> cast(x as string), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_map(x -> cast(x as string), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_map(x -> cast(x as string), array_range(number))[number] FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_map(x -> array_range(x), array_range(number))[2] FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_map(x -> array_range(x), array_range(number))[-1] FROM numbers LIMIT 10;"
    order_qt_sql "SELECT array_map(x -> array_range(x), array_range(number))[number] FROM numbers LIMIT 10;"

    // ============= array_enumerate_uniq =========
    qt_order_qt_sql "SELECT 'array_enumerate_uniq';"
    order_qt_sql """ SELECT array_enumerate_uniq(array_enumerate_uniq(array(cast(10 as LargeInt), cast(100 as LargeInt), cast(2 as LargeInt))), array(cast(123 as LargeInt), cast(1023 as LargeInt), cast(123 as LargeInt))); """

    order_qt_sql """SELECT array_enumerate_uniq(
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666],
            [111111, 222222, 333333],
            [444444, 555555, 666666]);"""
    order_qt_sql """SELECT array_enumerate_uniq(array(STDDEV_SAMP(910947.571364)), array(NULL)) from numbers;"""
    order_qt_sql """ SELECT max(arrayJoin(arrayEnumerateUniq(arrayMap(x -> intDiv(x, 10), URLCategories)))) FROM test.hits """
    order_qt_sql """ SELECT max(arrayJoin(arr)) FROM (SELECT arrayEnumerateUniq(group_array(DIV(number, 54321)) AS nums, group_array(cast(DIV(number, 98765) as string))) AS arr FROM (SELECT number FROM numbers LIMIT 1000000) GROUP BY bitmap_hash(number) % 100000);"""

    qt_order_qt_sql """SELECT 'array_concat_slice_push_back_push_front_pop_back_pop_front'"""
    order_qt_sql "SELECT array_concat([]);"
    order_qt_sql "SELECT array_concat([], []);"
    order_qt_sql "SELECT array_concat([], [], []);"
    order_qt_sql "SELECT array_concat([Null], []);"
    order_qt_sql "SELECT array_concat([Null], [], [1]);"
    order_qt_sql "SELECT array_concat([1, 2], [-1, -2], [0.3, 0.7], [Null]);"
    order_qt_sql "SELECT array_concat(Null, []);"
    order_qt_sql "SELECT array_concat([1], [-1], Null);"
    order_qt_sql "SELECT array_concat([1, 2], [3, 4]);"
    order_qt_sql "SELECT array_concat([1], [2, 3, 4]);"
    order_qt_sql "SELECT array_concat([], []);"
    order_qt_sql " SELECT array_concat(['abc'], ['def', 'gh', 'qwe']);"
    order_qt_sql " SELECT array_concat([1, NULL, 2], [3, NULL, 4]);"
    order_qt_sql "SELECT array_concat([1, Null, 2], [3, 4]);"

    order_qt_sql "SELECT array_slice(Null, 1, 2);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, Null);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, Null);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, 4);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], Null, -2);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -3, Null);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, 3);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, -2);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -4, 2);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -4, -2);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 2, 0);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -10, 15);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -15, 10);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], -15, 9);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, 0);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, -1);"
    order_qt_sql "SELECT array_slice([1, 2, 3, 4, 5, 6], 10, 1);"
    order_qt_sql "SELECT array_slice([1, 2, Null, 4, 5, 6], 2, 4);"
    order_qt_sql "SELECT array_slice(['a', 'b', 'c', 'd', 'e'], 2, 3);"
    order_qt_sql "SELECT array_slice([Null, 'b', Null, 'd', 'e'], 2, 3);"
    order_qt_sql "SELECT array_slice([], [NULL], NULL), 1 from numbers limit 2;"

    order_qt_sql "SELECT array_pushback(Null, 1);"
    order_qt_sql "SELECT array_pushback([1], 1);"
    order_qt_sql "SELECT array_pushback([Null], 1);"
    order_qt_sql "SELECT array_pushback([0.5, 0.7], 1);"
    order_qt_sql "SELECT array_pushback([1], -1);"
    order_qt_sql "SELECT array_pushback(['a', 'b'], 'cd');"
    order_qt_sql "SELECT array_pushback([], 1);"
    order_qt_sql "SELECT array_pushback([], -1);"
    
    order_qt_sql "SELECT array_pushfront(Null, 1);"
    order_qt_sql "SELECT array_pushfront([1], 1);"
    order_qt_sql "SELECT array_pushfront([Null], 1);"
    order_qt_sql "SELECT array_pushfront([0.5, 0.7], 1);"
    order_qt_sql "SELECT array_pushfront([1], -1);"
    order_qt_sql "SELECT array_pushfront(['a', 'b'], 'cd');"
    order_qt_sql "SELECT array_pushfront([], 1);"
    order_qt_sql "SELECT array_pushfront([], -1);"

    order_qt_sql "SELECT array_popback(Null);"
    order_qt_sql "SELECT array_popback([]);"
    order_qt_sql "SELECT array_popback([1]);"
    order_qt_sql "SELECT array_popback([1, 2, 3]);"
    order_qt_sql "SELECT array_popback([0.1, 0.2, 0.3]);"
    order_qt_sql "SELECT array_popback(['a', 'b', 'c']);"
    
    order_qt_sql "SELECT array_popfront(Null);"
    order_qt_sql "SELECT array_popfront([]);"
    order_qt_sql "SELECT array_popfront([1]);"
    order_qt_sql "SELECT array_popfront([1, 2, 3]);"
    order_qt_sql "SELECT array_popfront([0.1, 0.2, 0.3]);"
    order_qt_sql "SELECT array_popfront(['a', 'b', 'c']);"


    // ============= array_compact =========
    sql "SELECT 'array_compact';"
    order_qt_sql "SELECT array_compact([[[]], [[], []], [[], []], [[]]]);"
    order_qt_sql "SELECT array_compact(array_map(x -> (cast(x DIV 3) as string), array_range(number))) FROM numbers;"
    order_qt_sql "SELECT array_compact(array_map(x->0, [NULL]));"
    order_qt_sql "SELECT cast(array_compact(array_map(x->0, [NULL])) as string);"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((number, intDiv(number, 3) % 3))) FROM numbers(10);"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((toString(number), toString(intDiv(number, 3) % 3)))) FROM numbers(10);"
//    order_qt_sql "SELECT array_compact(x -> x.2, groupArray((toString(number), intDiv(number, 3) % 3))) FROM numbers(10);"

    // ============= array_difference =========
    //  Overflow is Ok and behaves as the CPU does it.
    order_qt_sql "SELECT array_difference([65536, -9223372036854775808]);"
    // Diff of unsigned int -> int
    order_qt_sql "SELECT array_difference( cast([10, 1] as Array<int>));"
    order_qt_sql "SELECT array_difference( cast([10, 1] as Array<BIGINT>));"
    order_qt_sql "SELECT array_difference( cast([10, 1] as Array<LargeInt>));"

    // now we not support array with in/comparable predict
//    order_qt_sql "SELECT [1] < [1000], ['abc'] = [NULL], ['abc'] = [toNullable('abc')], [[]] = [[]], [[], [1]] > [[], []], [[1]] < [[], []], [[], []] > [[]],[([], ([], []))] < [([], ([], ['hello']))];"

    order_qt_sql "SELECT array_difference(array(cast(100.0000991821289 as Decimal), -2147483647)) AS x;"

}
