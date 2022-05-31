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
suite("test_bitmap_index", "index") {
    def tbName1 = "test_bitmap_index_dup"
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR,
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
    String res = "null"
    sql "CREATE INDEX IF NOT EXISTS index1 ON ${tbName1} (k1) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index2 ON ${tbName1} (k2) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index3 ON ${tbName1} (k3) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index4 ON ${tbName1} (k4) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index5 ON ${tbName1} (k5) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index6 ON ${tbName1} (k6) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index7 ON ${tbName1} (k7) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index8 ON ${tbName1} (k8) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index9 ON ${tbName1} (k9) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index10 ON ${tbName1} (k10) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index11 ON ${tbName1} (k11) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "insert into ${tbName1} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1);"
    qt_sql "desc ${tbName1};"
    qt_sql "SHOW INDEX FROM ${tbName1};"
    qt_sql "select * from ${tbName1};"
    res = "null"
    sql "DROP INDEX IF EXISTS index1 ON ${tbName1};"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "DROP TABLE ${tbName1};"

    def tbName2 = "test_bitmap_index_agg"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR,
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN,
                v1 INT SUM
            )
            AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index1 ON ${tbName2} (k1) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index2 ON ${tbName2} (k2) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index3 ON ${tbName2} (k3) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index4 ON ${tbName2} (k4) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index5 ON ${tbName2} (k5) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index6 ON ${tbName2} (k6) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index7 ON ${tbName2} (k7) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index8 ON ${tbName2} (k8) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index9 ON ${tbName2} (k9) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index10 ON ${tbName2} (k10) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index11 ON ${tbName2} (k11) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "insert into ${tbName2} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,1);"
    qt_sql "desc ${tbName2};"
    qt_sql "SHOW INDEX FROM ${tbName2};"
    qt_sql "select * from ${tbName2};"
    res = "null"
    sql "DROP INDEX IF EXISTS index1 ON ${tbName2};"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "DROP TABLE ${tbName2};"

    def tbName3 = "test_bitmap_index_unique"
    sql "DROP TABLE IF EXISTS ${tbName3}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName3} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR,
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN,
               v1  INT
            )
            UNIQUE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index1 ON ${tbName3} (k1) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index2 ON ${tbName3} (k2) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index3 ON ${tbName3} (k3) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index4 ON ${tbName3} (k4) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index5 ON ${tbName3} (k5) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index6 ON ${tbName3} (k6) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index7 ON ${tbName3} (k7) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index8 ON ${tbName3} (k8) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index9 ON ${tbName3} (k9) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index10 ON ${tbName3} (k10) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "CREATE INDEX IF NOT EXISTS index11 ON ${tbName3} (k11) USING BITMAP;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "insert into ${tbName3} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,1);"
    qt_sql "desc ${tbName3};"
    qt_sql "SHOW INDEX FROM ${tbName3};"
    qt_sql "select * from ${tbName3};"
    res = "null"
    sql "DROP INDEX IF EXISTS index1 ON ${tbName3};"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName3}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "DROP TABLE ${tbName3};"
}

