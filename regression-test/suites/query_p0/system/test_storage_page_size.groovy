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

suite ("test_storage_page_size") {
    sql """ DROP TABLE IF EXISTS table_1; """
    sql """
        create table table_1 (
            k1 int not null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        distributed BY random buckets 1
        properties("replication_num" = "1");
    """
    test {
        sql "show create table table_1;"
        check { result, exception, startTime, endTime ->
            assertFalse(result[0][1].contains("storage_page_size"))
        }
    }

    // min - 1
    sql """ DROP TABLE IF EXISTS table_2; """
    test {
        sql """
            create table table_2 (
                k1 int not null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1)
            distributed BY random buckets 1
            properties("replication_num" = "1", "storage_page_size" = "4095");
        """
        check { result, exception, startTime, endTime ->
            if (exception) {
                assert exception.message.contains("Storage page size must be between 4KB and 10MB.")
            }
        }
    }

    // min
    sql """ DROP TABLE IF EXISTS table_3; """
    sql """
        create table table_3 (
            k1 int not null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        distributed BY random buckets 1
        properties("replication_num" = "1", "storage_page_size" = "4096");
    """
    test {
        sql "show create table table_3;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"storage_page_size\" = \"4096\""))
        }
    }


    // min + 1
    sql """ DROP TABLE IF EXISTS table_4; """
    sql """
        create table table_4 (
            k1 int not null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        distributed BY random buckets 1
        properties("replication_num" = "1", "storage_page_size" = "4097");
    """
    test {
        sql "show create table table_4;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"storage_page_size\" = \"8192\""))
        }
    }

    // 65537
    sql """ DROP TABLE IF EXISTS table_5; """
    sql """
        create table table_5 (
            k1 int not null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        distributed BY random buckets 1
        properties("replication_num" = "1", "storage_page_size" = "65537");
    """
    test {
        sql "show create table table_5;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"storage_page_size\" = \"69632\""))
        }
    }
    test {
        sql """ alter table table_5 set ("storage_page_size" = "65535"); """
        check { result, exception, startTime, endTime ->
            if (exception) {
                assert exception.message.contains("You can not modify storage_page_size")
            }
        }
    }

    // max - 1
    sql """ DROP TABLE IF EXISTS table_6; """
    sql """
        create table table_6 (
            k1 int not null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        distributed BY random buckets 1
        properties("replication_num" = "1", "storage_page_size" = "10485759");
    """
    test {
        sql "show create table table_6;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"storage_page_size\" = \"10485760\""))
        }
    }

    // max
    sql """ DROP TABLE IF EXISTS table_7; """
    sql """
        create table table_7 (
            k1 int not null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        distributed BY random buckets 1
        properties("replication_num" = "1", "storage_page_size" = "10485760");
    """
    test {
        sql "show create table table_7;"
        check { result, exception, startTime, endTime ->
            assertTrue(result[0][1].contains("\"storage_page_size\" = \"10485760\""))
        }
    }

    // max + 1
    sql """ DROP TABLE IF EXISTS table_8; """
    test {
        sql """
            create table table_8 (
                k1 int not null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1)
            distributed BY random buckets 1
            properties("replication_num" = "1", "storage_page_size" = "10485761");
        """
        check { result, exception, startTime, endTime ->
            if (exception) {
                assert exception.message.contains("Storage page size must be between 4KB and 10MB.")
            }
        }
    }
}
