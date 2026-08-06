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
// "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_clone_legacy_nullable_from_nereids") {
    // the agg will cause legacy expr clone, but old code expr clone no set nullableFromNereids,
    // then it will cause error:
    // Could not find function lt, arg (CAST col_date_undef_signed_not_null(DateV2) TO DateTimeV2(0)): DateTimeV2(0),
    // (CAST VectorizedFnCall[hours_sub](arguments=col_datetime_6__undef_signed_not_null, INT,return=DateTimeV2(6))(DateTimeV2(6)) TO Nullable(DateTimeV2(0))):
    // Nullable(DateTimeV2(0)) return BOOL
    sql """
        drop table if exists tbl_test_clone_legacy_nullable_from_nereids force;
        create table tbl_test_clone_legacy_nullable_from_nereids (
            col_datetime_6__undef_signed_not_null datetime(6)  not null  ,
            pk int,
            col_date_undef_signed date  null  ,
            col_date_undef_signed_not_null date  not null  ,
            col_datetime_undef_signed datetime  null  ,
            col_datetime_undef_signed_not_null datetime  not null  ,
            col_datetime_3__undef_signed datetime(3)  null  ,
            col_datetime_3__undef_signed_not_null datetime(3)  not null  ,
            col_datetime_6__undef_signed datetime(6)  null  
         ) engine=olap
        DUPLICATE KEY(col_datetime_6__undef_signed_not_null, pk)
        PARTITION BY         RANGE(col_datetime_6__undef_signed_not_null) (
                    PARTITION p0 VALUES LESS THAN ("1997-01-01 00:00:00"),
                    PARTITION p1 VALUES LESS THAN ("2010-01-01 00:00:00"),
                    PARTITION p2 VALUES LESS THAN ("2020-01-01 00:00:00"),
                    PARTITION p3 VALUES LESS THAN ("2030-01-01 00:00:00"),
                    PARTITION p4 VALUES LESS THAN ("2040-01-01 00:00:00"),
                    PARTITION p5 VALUES LESS THAN ("2050-01-01 00:00:00"),
                    PARTITION p6 VALUES LESS THAN ("2060-01-01 00:00:00"),
                    PARTITION p7 VALUES LESS THAN ("2070-01-01 00:00:00"),
                    PARTITION p8 VALUES LESS THAN ("2180-01-01 00:00:00"),
                    PARTITION p9 VALUES LESS THAN ("9999-01-01 00:00:00"),
                    PARTITION p10 VALUES LESS THAN (MAXVALUE)
                )
            
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into tbl_test_clone_legacy_nullable_from_nereids(
                pk,col_date_undef_signed,col_date_undef_signed_not_null,col_datetime_undef_signed,
                col_datetime_undef_signed_not_null,col_datetime_3__undef_signed,
                col_datetime_3__undef_signed_not_null,col_datetime_6__undef_signed,col_datetime_6__undef_signed_not_null)
        values (0,'2024-08-03 13:08:30','2010-01-21 21:39:01',null,'2014-08-12','9999-12-31','2013-07-20','2014-08-12','2000-03-12');
    """

    order_qt_select """
        select window_funnel(4941185, "fixed", col_datetime_undef_signed,
                col_date_undef_signed_not_null < CAST(DATE_SUB(col_datetime_6__undef_signed_not_null, INTERVAL 2 HOUR) AS DATETIME)) AS col_alias4750
        from tbl_test_clone_legacy_nullable_from_nereids
    """

    sql """
        drop table if exists tbl_test_clone_legacy_nullable_from_nereids force;
    """
}
