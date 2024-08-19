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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("topn_alltype") {
   qt_boolean"""
   select * from (select id from expr_test order by kbool,id limit 3)t order by id;
   """
    qt_boolean"""
   select * from (select id from expr_test order by kbool,id limit 15)t order by id;
   """

    qt_tinyint"""
   select * from (select id from expr_test order by ktint,id limit 3)t order by id;
   """
    qt_tinyint"""
   select * from (select id from expr_test order by ktint,id limit 15)t order by id;
   """

    qt_smallint"""
   select * from (select id from expr_test order by ksint,id limit 3)t order by id;
   """
    qt_smallint"""
   select * from (select id from expr_test order by ksint,id limit 15)t order by id;
   """

    qt_int"""
   select * from (select id from expr_test order by kint,id limit 3)t order by id;
   """
    qt_int"""
   select * from (select id from expr_test order by kint,id limit 15)t order by id;
   """

    qt_bigint"""
   select * from (select id from expr_test order by kbint,id limit 3)t order by id;
   """
    qt_bigint"""
   select * from (select id from expr_test order by kbint,id limit 15)t order by id;
   """

    qt_largeint"""
   select * from (select id from expr_test order by klint,id limit 3)t order by id;
   """
    qt_largeint"""
   select * from (select id from expr_test order by klint,id limit 15)t order by id;
   """

    qt_varchar"""
   select * from (select id from expr_test order by kvchr,id limit 3)t order by id;
   """
    qt_varchar"""
   select * from (select id from expr_test order by kvchr,id limit 15)t order by id;
   """

    qt_char"""
   select * from (select id from expr_test order by kchr,id limit 3)t order by id;
   """
    qt_char"""
   select * from (select id from expr_test order by kchr,id limit 15)t order by id;
   """

    qt_str"""
   select * from (select id from expr_test order by kstr,id limit 3)t order by id;
   """
    qt_str"""
   select * from (select id from expr_test order by kstr,id limit 15)t order by id;
   """

    qt_date"""
   select * from (select id from expr_test order by kdt,id limit 3)t order by id;
   """
    qt_date"""
   select * from (select id from expr_test order by kdt,id limit 15)t order by id;
   """

    qt_datev2"""
   select * from (select id from expr_test order by kdtv2,id limit 3)t order by id;
   """
    qt_datev2"""
   select * from (select id from expr_test order by kdtv2,id limit 15)t order by id;
   """

    qt_datetime"""
   select * from (select id from expr_test order by kdtm,id limit 3)t order by id;
   """
    qt_datetime"""
   select * from (select id from expr_test order by kdtm,id limit 15)t order by id;
   """

    qt_datetimev2"""
   select * from (select id from expr_test order by kdtmv2,id limit 3)t order by id;
   """
    qt_datetimev2"""
   select * from (select id from expr_test order by kdtmv2,id limit 15)t order by id;
   """

    qt_decimal32"""
   select * from (select id from expr_test order by kdcml32v3,id limit 3)t order by id;
   """
    qt_decimal32"""
   select * from (select id from expr_test order by kdcml32v3,id limit 15)t order by id;
   """

    qt_decimal64"""
   select * from (select id from expr_test order by kdcml64v3,id limit 3)t order by id;
   """
    qt_decimal64"""
   select * from (select id from expr_test order by kdcml64v3,id limit 15)t order by id;
   """

    qt_decimal128"""
   select * from (select id from expr_test order by kdcml128v3,id limit 3)t order by id;
   """
    qt_decimal128"""
   select * from (select id from expr_test order by kdcml128v3,id limit 15)t order by id;
   """

    qt_decimalv2"""
   select * from (select id from expr_test order by kdtmv2,id limit 3)t order by id;
   """
    qt_decimalv2"""
   select * from (select id from expr_test order by kdtmv2,id limit 15)t order by id;
   """

   sql "set enable_decimal256=true;"
   qt_decimal256"""
   select * from (select id from expr_test2 order by kdcml256v3,id limit 3)t order by id;
   """
    qt_decimal256"""
   select * from (select id from expr_test2 order by kdcml256v3,id limit 5)t order by id;
   """

   qt_ipv4"""
   select * from (select id from expr_test2 order by kipv4,id limit 3)t order by id;
   """
    qt_ipv4"""
   select * from (select id from expr_test2 order by kipv4,id limit 5)t order by id;
   """
   qt_ipv6"""
   select * from (select id from expr_test2 order by kipv6,id limit 3)t order by id;
   """
    qt_ipv6"""
   select * from (select id from expr_test2 order by kipv6,id limit 5)t order by id;
   """

  

   // not nullable
   qt_boolean"""
   select * from (select id from expr_test_not_nullable order by kbool,id limit 3)t order by id;
   """
    qt_boolean"""
   select * from (select id from expr_test_not_nullable order by kbool,id limit 15)t order by id;
   """

    qt_tinyint"""
   select * from (select id from expr_test_not_nullable order by ktint,id limit 3)t order by id;
   """
    qt_tinyint"""
   select * from (select id from expr_test_not_nullable order by ktint,id limit 15)t order by id;
   """

    qt_smallint"""
   select * from (select id from expr_test_not_nullable order by ksint,id limit 3)t order by id;
   """
    qt_smallint"""
   select * from (select id from expr_test_not_nullable order by ksint,id limit 15)t order by id;
   """

    qt_int"""
   select * from (select id from expr_test_not_nullable order by kint,id limit 3)t order by id;
   """
    qt_int"""
   select * from (select id from expr_test_not_nullable order by kint,id limit 15)t order by id;
   """

    qt_bigint"""
   select * from (select id from expr_test_not_nullable order by kbint,id limit 3)t order by id;
   """
    qt_bigint"""
   select * from (select id from expr_test_not_nullable order by kbint,id limit 15)t order by id;
   """

    qt_largeint"""
   select * from (select id from expr_test_not_nullable order by klint,id limit 3)t order by id;
   """
    qt_largeint"""
   select * from (select id from expr_test_not_nullable order by klint,id limit 15)t order by id;
   """

    qt_varchar"""
   select * from (select id from expr_test_not_nullable order by kvchr,id limit 3)t order by id;
   """
    qt_varchar"""
   select * from (select id from expr_test_not_nullable order by kvchr,id limit 15)t order by id;
   """

    qt_char"""
   select * from (select id from expr_test_not_nullable order by kchr,id limit 3)t order by id;
   """
    qt_char"""
   select * from (select id from expr_test_not_nullable order by kchr,id limit 15)t order by id;
   """

    qt_str"""
   select * from (select id from expr_test_not_nullable order by kstr,id limit 3)t order by id;
   """
    qt_str"""
   select * from (select id from expr_test_not_nullable order by kstr,id limit 15)t order by id;
   """

    qt_date"""
   select * from (select id from expr_test_not_nullable order by kdt,id limit 3)t order by id;
   """
    qt_date"""
   select * from (select id from expr_test_not_nullable order by kdt,id limit 15)t order by id;
   """

    qt_datev2"""
   select * from (select id from expr_test_not_nullable order by kdtv2,id limit 3)t order by id;
   """
    qt_datev2"""
   select * from (select id from expr_test_not_nullable order by kdtv2,id limit 15)t order by id;
   """

    qt_datetime"""
   select * from (select id from expr_test_not_nullable order by kdtm,id limit 3)t order by id;
   """
    qt_datetime"""
   select * from (select id from expr_test_not_nullable order by kdtm,id limit 15)t order by id;
   """

    qt_datetimev2"""
   select * from (select id from expr_test_not_nullable order by kdtmv2,id limit 3)t order by id;
   """
    qt_datetimev2"""
   select * from (select id from expr_test_not_nullable order by kdtmv2,id limit 15)t order by id;
   """

    qt_decimal32"""
   select * from (select id from expr_test_not_nullable order by kdcml32v3,id limit 3)t order by id;
   """
    qt_decimal32"""
   select * from (select id from expr_test_not_nullable order by kdcml32v3,id limit 15)t order by id;
   """

    qt_decimal64"""
   select * from (select id from expr_test_not_nullable order by kdcml64v3,id limit 3)t order by id;
   """
    qt_decimal64"""
   select * from (select id from expr_test_not_nullable order by kdcml64v3,id limit 15)t order by id;
   """

    qt_decimal128"""
   select * from (select id from expr_test_not_nullable order by kdcml128v3,id limit 3)t order by id;
   """
    qt_decimal128"""
   select * from (select id from expr_test_not_nullable order by kdcml128v3,id limit 15)t order by id;
   """

    qt_decimalv2"""
   select * from (select id from expr_test_not_nullable order by kdtmv2,id limit 3)t order by id;
   """
    qt_decimalv2"""
   select * from (select id from expr_test_not_nullable order by kdtmv2,id limit 15)t order by id;
   """

   sql "set enable_decimal256=true;"
   qt_decimal256"""
   select * from (select id from expr_test_not_nullable2 order by kdcml256v3,id limit 3)t order by id;
   """
    qt_decimal256"""
   select * from (select id from expr_test_not_nullable2 order by kdcml256v3,id limit 5)t order by id;
   """

   qt_ipv4"""
   select * from (select id from expr_test_not_nullable2 order by kipv4,id limit 3)t order by id;
   """
    qt_ipv4"""
   select * from (select id from expr_test_not_nullable2 order by kipv4,id limit 5)t order by id;
   """
   qt_ipv6"""
   select * from (select id from expr_test_not_nullable2 order by kipv6,id limit 3)t order by id;
   """
    qt_ipv6"""
   select * from (select id from expr_test_not_nullable2 order by kipv6,id limit 5)t order by id;
   """

   // expr
    qt_boolean"""
   select * from (select id from expr_test order by abs(kbool),id limit 3)t order by id;
   """
    qt_boolean"""
   select * from (select id from expr_test order by abs(kbool),id limit 15)t order by id;
   """

    qt_tinyint"""
   select * from (select id from expr_test order by abs(ktint),id limit 3)t order by id;
   """
    qt_tinyint"""
   select * from (select id from expr_test order by abs(ktint),id limit 15)t order by id;
   """

    qt_smallint"""
   select * from (select id from expr_test order by abs(ksint),id limit 3)t order by id;
   """
    qt_smallint"""
   select * from (select id from expr_test order by abs(ksint),id limit 15)t order by id;
   """

    qt_int"""
   select * from (select id from expr_test order by abs(kint),id limit 3)t order by id;
   """
    qt_int"""
   select * from (select id from expr_test order by abs(kint),id limit 15)t order by id;
   """

    qt_bigint"""
   select * from (select id from expr_test order by abs(kbint),id limit 3)t order by id;
   """
    qt_bigint"""
   select * from (select id from expr_test order by abs(kbint),id limit 15)t order by id;
   """

    qt_largeint"""
   select * from (select id from expr_test order by abs(klint),id limit 3)t order by id;
   """
    qt_largeint"""
   select * from (select id from expr_test order by abs(klint),id limit 15)t order by id;
   """

    qt_varchar"""
   select * from (select id from expr_test order by repeat(kvchr,2),id limit 3)t order by id;
   """
    qt_varchar"""
   select * from (select id from expr_test order by repeat(kvchr,2),id limit 15)t order by id;
   """

    qt_char"""
   select * from (select id from expr_test order by repeat(kchr,2),id limit 3)t order by id;
   """
    qt_char"""
   select * from (select id from expr_test order by repeat(kchr,2),id limit 15)t order by id;
   """

    qt_str"""
   select * from (select id from expr_test order by repeat(kstr,2),id limit 3)t order by id;
   """
    qt_str"""
   select * from (select id from expr_test order by repeat(kstr,2),id limit 15)t order by id;
   """

    qt_date"""
   select * from (select id from expr_test order by hour_floor(kdt),id limit 3)t order by id;
   """
    qt_date"""
   select * from (select id from expr_test order by hour_floor(kdt),id limit 15)t order by id;
   """

    qt_datev2"""
   select * from (select id from expr_test order by hour_floor(kdtv2),id limit 3)t order by id;
   """
    qt_datev2"""
   select * from (select id from expr_test order by hour_floor(kdtv2),id limit 15)t order by id;
   """

    qt_datetime"""
   select * from (select id from expr_test order by hour_floor(kdtm),id limit 3)t order by id;
   """
    qt_datetime"""
   select * from (select id from expr_test order by hour_floor(kdtm),id limit 15)t order by id;
   """

    qt_datetimev2"""
   select * from (select id from expr_test order by hour_floor(kdtmv2),id limit 3)t order by id;
   """
    qt_datetimev2"""
   select * from (select id from expr_test order by hour_floor(kdtmv2),id limit 15)t order by id;
   """


    qt_decimal32"""
   select * from (select id from expr_test order by abs(kdcml32v3),id limit 3)t order by id;
   """
    qt_decimal32"""
   select * from (select id from expr_test order by abs(kdcml32v3),id limit 15)t order by id;
   """

    qt_decimal64"""
   select * from (select id from expr_test order by abs(kdcml64v3),id limit 3)t order by id;
   """
    qt_decimal64"""
   select * from (select id from expr_test order by abs(kdcml64v3),id limit 15)t order by id;
   """

    qt_decimal128"""
   select * from (select id from expr_test order by abs(kdcml128v3),id limit 3)t order by id;
   """
    qt_decimal128"""
   select * from (select id from expr_test order by abs(kdcml128v3),id limit 15)t order by id;
   """

    qt_decimalv2"""
   select * from (select id from expr_test order by abs(kdtmv2),id limit 3)t order by id;
   """
    qt_decimalv2"""
   select * from (select id from expr_test order by abs(kdtmv2),id limit 15)t order by id;
   """

   sql "set enable_decimal256=true;"
   qt_decimal256"""
   select * from (select id from expr_test2 order by abs(kdcml256v3),id limit 3)t order by id;
   """
    qt_decimal256"""
   select * from (select id from expr_test2 order by abs(kdcml256v3),id limit 5)t order by id;
   """

}
