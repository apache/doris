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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

String[] getFiles(String dirName, int num) {
    File[] datas = new File(dirName).listFiles()
    if (num != datas.length) {
        throw new Exception("num not equals,expect:" + num + " vs real:" + datas.length)
    }
    String[] array = new String[datas.length];
    for (int i = 0; i < datas.length; i++) {
        array[i] = datas[i].getPath();
    }
    Arrays.sort(array);
    return array;
}

suite("test_group_commit_insert_into_lineitem_multiple_table") {
    String[] file_array;
    def prepare = {
        def dataDir = "${context.config.cacheDataPath}/insert_into_lineitem_multiple_table"
        File dir = new File(dataDir)
        if (!dir.exists()) {
            new File(dataDir).mkdir()
            logger.info("download lineitem")
            def download_file = """/usr/bin/curl ${getS3Url()}/regression/tpch/sf1/lineitem.tbl.1
--output ${dataDir}/lineitem.tbl.1""".execute().getText()
            def split_file = """split -l 60000 ${dataDir}/lineitem.tbl.1 ${dataDir}/""".execute().getText()
            def rm_file = """rm ${dataDir}/lineitem.tbl.1""".execute().getText()
        }
        file_array = getFiles(dataDir, 11)
        for (String s : file_array) {
            logger.info(s)
        }
    }
    def insert_table_base = "test_insert_into_lineitem_multiple_table"
    def batch = 100;
    def total = 0;
    def rwLock = new ReentrantReadWriteLock();
    def wlock = rwLock.writeLock();

    def getRowCount = { expectedRowCount, table_name ->
        def retry = 0
        while (retry < 60) {
            try {
                def rowCount = sql "select count(*) from ${table_name}"
                logger.info("rowCount: " + rowCount + ", retry: " + retry)
                if (rowCount[0][0] >= expectedRowCount) {
                    break
                }
            } catch (Exception e) {
                logger.info("select count get exception", e);
            }
            Thread.sleep(5000)
            retry++
        }
    }

    def create_insert_table = { table_name ->
        // create table
        sql """ drop table if exists ${table_name}; """

        sql """
   CREATE TABLE ${table_name} (
    l_shipdate    DATEV2 NOT NULL,
    l_orderkey    bigint NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimalv3(15, 2) NOT NULL,
    l_extendedprice  decimalv3(15, 2) NOT NULL,
    l_discount    decimalv3(15, 2) NOT NULL,
    l_tax         decimalv3(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATEV2 NOT NULL,
    l_receiptdate DATEV2 NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);
        """
        sql """ set group_commit = async_mode; """
        sql """ set enable_nereids_dml = false; """
    }

    def do_insert_into = { exp_str, num ->
        def i = 0;
        while (true) {
            try {
                def result = insert_into_sql(exp_str, num);
                logger.info("result:" + result);
                break
            } catch (Exception e) {
                logger.info("got exception:" + e)
            }
            i++;
            if (i >= 30) {
                throw new Exception("""fail to much time""")
            }
        }
    }

    def insert_file = { file_name, table_name ->
        sql """ set group_commit = async_mode; """
        sql """ set enable_nereids_dml = false; """
        logger.info("file:" + file_name)
        //read and insert
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file_name));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        String s = null;
        StringBuilder sb = null;
        int c = 0;
        int t = 0;
        while (true) {
            try {
                if (c == batch) {
                    sb.append(";");
                    String exp = sb.toString();
                    do_insert_into(exp, c);
                    c = 0;
                }
                s = reader.readLine();
                if (s != null) {
                    if (c == 0) {
                        sb = new StringBuilder();
                        sb.append("insert into ${table_name} (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                    }
                    if (c > 0) {
                        sb.append(",");
                    }
                    String[] array = s.split("\\|");
                    sb.append("(");
                    for (int i = 0; i < array.length; i++) {
                        sb.append("\"" + array[i] + "\"");
                        if (i != array.length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append(")");
                    c++;
                    t++;
                } else if (c > 0) {
                    sb.append(";");
                    String exp = sb.toString();
                    do_insert_into(exp, c);
                    break;
                } else {
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        {
            logger.info("t: " + t)
            wlock.lock()
            total += t;
            wlock.unlock()
        }

        if (reader != null) {
            reader.close();
        }
        getRowCount(t, table_name)
    }

    def process = {
        def threads = []
        for (int k = 0; k < file_array.length; k++) {
            int n = k;
            String file_name = file_array[n]
            String table_name = insert_table_base + "_" + n;
            create_insert_table(table_name)
            logger.info("insert into file:" + file_name)
            threads.add(Thread.startDaemon {
                insert_file(file_name, table_name)
            })
        }
        for (Thread th in threads) {
            th.join()
        }

        for (int k = 0; k < file_array.length; k++) {
            String table_name = insert_table_base + "_" + k;
            qt_sql """ select count(*) from ${table_name}; """
        }
    }

    try {
        prepare()
        process()
    } finally {

    }


}