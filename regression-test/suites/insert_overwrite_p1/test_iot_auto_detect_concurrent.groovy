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
    // only nereids now. default for 2.1 and later
    def db_name = "test_iot_auto_detect_concurrent"
    def table_name = "test_concurrent_write"

    sql " create database if not exists test_iot_auto_detect_concurrent; "
    sql " use test_iot_auto_detect_concurrent; "
    sql " drop table if exists test_concurrent_write; "
    sql new File("""${context.file.parent}/ddl/test_iot_auto_detect_concurrent.sql""").text

    def success_status = true
    def load_data = { range, offset, expect_success ->
        try {
            sql " use test_iot_auto_detect_concurrent; "
            sql """ insert overwrite table test_concurrent_write partition(*)
                        select number*10+${offset} from numbers("number" = "${range}");
            """
        } catch (Exception e) {
            if (expect_success) {
                success_status = false
                log.info("fails one")
            }
            log.info("successfully catch the failed insert")
            return
        }
        if (!expect_success) {
            success_status = false
        }
    }

    def dropping = true
    def drop_partition = {
        sql " use test_iot_auto_detect_concurrent; "
        while (dropping) {
            try {
                sql """ alter table test_concurrent_write
                    drop partition p10, drop partition p20, drop partition p30, drop partition p40, drop partition p50,
                    drop partition p60, drop partition p70, drop partition p80, drop partition p90, drop partition p100;
                """
            } catch (Exception e) {}
        }
    }

    def result


    /// same data and partitions
    success_status = true
    sql """ insert into test_concurrent_write select * from numbers("number" = "1000"); """
    def thread1 = Thread.start { load_data(100, 0, false) }
    def thread2 = Thread.start { load_data(100, 0, false) }
    def thread3 = Thread.start { load_data(100, 0, false) }
    def thread4 = Thread.start { load_data(100, 0, false) }
    def thread5 = Thread.start { load_data(100, 0, false) }
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    // suppose result: success zero or one
    if (success_status) { // success zero
        log.info("test 1: success zero")
        result = sql " select count(k0) from test_concurrent_write; "
        assertEquals(result[0][0], 1000)
        result = sql " select count(distinct k0) from test_concurrent_write; "
        assertEquals(result[0][0], 1000)
    } else { // success one
        log.info("test 1: success one")
        result = sql " select count(k0) from test_concurrent_write; "
        assertEquals(result[0][0], 100)
        result = sql " select count(distinct k0) from test_concurrent_write; "
        assertEquals(result[0][0], 100)
    }


    /// not same data/partitions
    success_status = true
    sql """ insert overwrite table test_concurrent_write select * from numbers("number" = "1000"); """
    def thread6 = Thread.start { load_data(50, 0, true) } // 0, 10 ... 490
    def thread7 = Thread.start { load_data(50, 500, true) } // 500, 10 ... 990
    thread6.join()
    thread7.join()
    // suppose result: Success to overwrite with a multiple of ten values
    assertTrue(success_status)
    qt_sql3 " select count(k0) from test_concurrent_write; "
    qt_sql4 " select count(distinct k0) from test_concurrent_write; "


    /// with drop partition concurrently
    success_status = true
    sql """ truncate table test_concurrent_write; """
    def thread10 = Thread.start { drop_partition() }
    def thread8 = Thread.start { load_data(100, 0, false) }
    def thread9 = Thread.start { load_data(100, 0, false) }
    thread8.join()
    thread9.join()
    dropping = false // stop dropping
    thread10.join()
    // no success insert occur
    assertTrue(success_status) // we concerned about this. no 
    qt_sql5 " select count(k0) from test_concurrent_write; "
}