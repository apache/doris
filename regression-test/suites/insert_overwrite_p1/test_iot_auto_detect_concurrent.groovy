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

suite("test_iot_auto_detect_concurrent") {
    // only nereids now
    sql """set enable_nereids_planner = true"""
    sql """set enable_fallback_to_original_planner = false"""
    sql """set enable_nereids_dml = true"""

    def db_name = "test_iot_auto_detect_concurrent"
    def table_name = "test_concurrent_write"

    sql " create database if not exists test_iot_auto_detect_concurrent; "
    sql " use test_iot_auto_detect_concurrent; "
    // sql " drop table if exists test_concurrent_write; "
    // sql new File("""${context.file.parent}/ddl/test_iot_auto_detect_concurrent.sql""").text

    def load_data = { range, offset ->
        sql """ insert overwrite table test_concurrent_write partition(*)
                    select number*10+${offset} from numbers("number" = "${range}");
        """
    }


    /// same data and partitions
    // sql """ insert overwrite table test_concurrent_write select * from numbers("number" = "1000"); """
    // def thread1 = Thread.start { load_data(100, 0) }
    // def thread2 = Thread.start { load_data(100, 0) }
    // def thread3 = Thread.start { load_data(100, 0) }
    // def thread4 = Thread.start { load_data(100, 0) }
    // def thread5 = Thread.start { load_data(100, 0) }
    // thread1.join()
    // thread2.join()
    // thread3.join()
    // thread4.join()
    // thread5.join()
    // // suppose result: Due to duplicate partition names, insert overwrite will fail all.
    // qt_sql " select count(k0) from test_concurrent_write; "
    // qt_sql " select count(distinct k0) from test_concurrent_write; "


    /// not same data/partitions
    sql """ insert overwrite table test_concurrent_write select * from numbers("number" = "1000"); """
    def thread6 = Thread.start { load_data(50, 0) } // 0, 10 ... 490
    def thread7 = Thread.start { load_data(50, 500) } // 0, 10 ... 500
    thread6.join()
    thread7.join()
    // suppose result: 
    qt_sql " select count(k0) from test_concurrent_write; "
    qt_sql " select count(distinct k0) from test_concurrent_write; "
    qt_sql " select * from test_concurrent_write order by k0; "
}