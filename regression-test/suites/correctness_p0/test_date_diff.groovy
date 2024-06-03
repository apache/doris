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

suite("test_date_diff") {
    sql """ drop table if exists dt6  """
    sql """
        create table dt6(
            k0 datetime(6) null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """
    insert into dt6 values ("0000-01-01 12:00:00"), ("0000-01-02 12:00:00"), ("0000-01-03 12:00:00"), ("0000-01-04 12:00:00"),
        ("0000-01-05 12:00:00"), ("0000-01-06 12:00:00"), ("0000-01-07 12:00:00"), ("0000-01-08 12:00:00");
    """
    qt_days """ select days_diff(k0, '0000-01-01 13:00:00') from dt6 order by k0; """
    qt_weeks """ select weeks_diff(k0, '0000-01-01 13:00:00') from dt6 order by k0; """
}