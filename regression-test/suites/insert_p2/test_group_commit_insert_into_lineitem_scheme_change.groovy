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

enum SC {
    TRUNCATE_TABLE(1),
    ADD_COLUMN(2),
    DELETE(3),
    DROP_COLUMN(4),
    CHANGE_ORDER(5)
    private int value

    SC(int value) {
        this.value = value
    }

    int getValue() {
        return value
    }
}

enum STATE {
    NORMAL(1),
    BEFORE_ADD_COLUMN(2),
    DROP_COLUMN(3)
    private int value

    STATE(int value) {
        this.value = value
    }

    int getValue() {
        return value
    }
}

String[] getFiles(String dirName, int num) {
    File[] datas = new File(dirName).listFiles()
    if (num != datas.length) {
        throw new Exception("num not equals,expect:" + num + " vs real:" + datas.length)
    }
    String[] tmp_array = new String[datas.length];
    for (int i = 0; i < datas.length; i++) {
        tmp_array[i] = datas[i].getPath();
    }
    Arrays.sort(tmp_array);
    String[] array = new String[5];
    for (int i = 0; i < 5; i++) {
        array[i] = tmp_array[i];
    }
    return array;
}

suite("test_group_commit_insert_into_lineitem_scheme_change") {
    String[] file_array;
    def prepare = {
        def dataDir = "${context.config.cacheDataPath}/insert_into_lineitem_scheme_change"
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
    def insert_table = "test_lineitem_scheme_change"
    def batch = 100;
    def count = 0;
    def total = 0;

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

    def create_insert_table_less_column = { table_name ->
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
                logger.info("msg:" + e.getMessage())
                logger.info("got exception:" + e)
                if (e.getMessage().contains("is blocked on schema change")) {
                    Thread.sleep(10000)
                }
            }
            i++;
            if (i >= 30) {
                throw new Exception("""fail to much time""")
            }
        }
    }

    def insert_data = { file_name, table_name, index ->
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file_name));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        String s = null;
        StringBuilder sb = null;
        count = 0;
        while (true) {
            try {
                if (count == batch) {
                    sb.append(";");
                    String exp = sb.toString();
                    do_insert_into(exp, count)
                    count = 0;
                }
                s = reader.readLine();
                if (s != null) {
                    if (count == 0) {
                        sb = new StringBuilder();
                        if (index == STATE.BEFORE_ADD_COLUMN.value) {
                            sb.append("insert into ${table_name} (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                        } else if (index == STATE.NORMAL.value) {
                            sb.append("insert into ${table_name} (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                        } else if (index == STATE.DROP_COLUMN.value) {
                            sb.append("insert into ${table_name} (l_orderkey, l_partkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment)VALUES");
                        }
                    }
                    if (count > 0) {
                        sb.append(",");
                    }
                    String[] array = s.split("\\|");
                    sb.append("(");
                    for (int i = 0; i < array.length; i++) {
                        if (index == STATE.BEFORE_ADD_COLUMN.value && i == 11) {
                            continue;
                        } else if (index == STATE.DROP_COLUMN.value && i == 2) {
                            continue;
                        }
                        sb.append("\"" + array[i] + "\"");
                        if (i != array.length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append(")");
                    count++;
                    total++;
                } else if (count > 0) {
                    sb.append(";");
                    String exp = sb.toString();
                    do_insert_into(exp, count)
                    break;
                } else {
                    break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (reader != null) {
            reader.close();
        }
    }

    def getAlterTableState = { table_name ->
        def retry = 0
        while (true) {
            def state = sql "show alter table column where tablename = '${table_name}' order by CreateTime desc "
            logger.info("alter table state: ${state}")
            logger.info("state:" + state[0][9]);
            if (state.size() > 0 && state[0][9] == "FINISHED") {
                return true
            }
            retry++
            if (retry >= 60) {
                return false
            }
            Thread.sleep(5000)
        }
        return false
    }

    def truncate = { table_name ->
        create_insert_table(table_name)
        total = 0;
        for (int i = 0; i < file_array.length; i++) {
            String fileName = file_array[i]
            logger.info("process file:" + fileName)
            if (i == (int) (file_array.length / 2)) {
                getRowCount(total, table_name)
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ truncate table ${table_name}; """
                        break
                    } catch (Exception e) {
                        logger.info("select count get exception", e);
                    }
                    retry++
                    Thread.sleep(2000)
                }
                total = 0;
            }
            insert_data(fileName, table_name, STATE.NORMAL.value)
        }
        logger.info("process truncate total:" + total)
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def delete = { table_name ->
        create_insert_table(table_name)
        total = 0;
        for (int i = 0; i < file_array.length; i++) {
            String fileName = file_array[i]
            logger.info("process file:" + fileName)
            if (i == (int) (file_array.length / 2)) {
                def retry = 0
                while (retry < 10) {
                    try {
                        def rowCount = sql """select count(*) from ${table_name} where l_orderkey >=10000;"""
                        log.info("rowCount:" + rowCount[0][0])
                        sql """ delete from ${table_name} where l_orderkey >=10000; """
                        total -= rowCount[0][0]
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    retry++
                    Thread.sleep(2000)
                }
            }
            insert_data(fileName, table_name, STATE.NORMAL.value)
        }
        logger.info("process delete total:" + total)
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def drop_column = { table_name ->
        create_insert_table(table_name)
        total = 0;
        for (int i = 0; i < file_array.length; i++) {
            String fileName = file_array[i]
            logger.info("process file:" + fileName)
            if (i == (int) (file_array.length / 2)) {
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ alter table ${table_name} DROP column l_suppkey; """
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    retry++
                    Thread.sleep(2000)
                }
            }
            if (i < (int) (file_array.length / 2)) {
                insert_data(fileName, table_name, STATE.NORMAL.value)
            } else {
                insert_data(fileName, table_name, STATE.DROP_COLUMN.value)
            }
        }
        logger.info("process drop column total:" + total)
        assertTrue(getAlterTableState(table_name), "drop column should success")
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def add_column = { table_name ->
        create_insert_table_less_column(table_name)
        total = 0;
        for (int i = 0; i < file_array.length; i++) {
            String fileName = file_array[i]
            logger.info("process file:" + fileName)
            if (i == (int) (file_array.length / 2)) {
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ alter table ${table_name} ADD column l_receiptdate DATEV2 after l_commitdate; """
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    retry++
                    Thread.sleep(2000)
                }
            }
            if (i < 5) {
                insert_data(fileName, table_name, STATE.BEFORE_ADD_COLUMN.value)
            } else {
                insert_data(fileName, table_name, STATE.NORMAL.value)
            }
        }
        logger.info("process add column total:" + total)
        assertTrue(getAlterTableState(table_name), "add column should success")
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }

    def change_order = { table_name ->
        create_insert_table(table_name)
        total = 0;
        for (int i = 0; i < file_array.length; i++) {
            String fileName = file_array[i]
            logger.info("process file:" + fileName)
            if (i == (int) (file_array.length / 2)) {
                def retry = 0
                while (retry < 10) {
                    try {
                        sql """ alter table ${table_name} order by (l_orderkey,l_shipdate,l_linenumber, l_partkey,l_suppkey,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment); """
                        break
                    } catch (Exception e) {
                        log.info("exception:", e)
                    }
                    retry++
                    Thread.sleep(2000)
                }
            }
            insert_data(fileName, table_name, STATE.NORMAL.value)
        }
        logger.info("process change order total:" + total)
        assertTrue(getAlterTableState(table_name), "modify column order should success")
        getRowCount(total, table_name)
        qt_sql """ select count(*) from ${table_name}; """
    }


    def process = { table_name ->
        for (int i = 1; i <= 5; i++) {
            switch (i) {
                case SC.TRUNCATE_TABLE.value:
                    truncate(table_name)
                    break
                case SC.DELETE.value:
                    delete(table_name)
                    break
                case SC.DROP_COLUMN.value:
                    drop_column(table_name)
                    break
                case SC.ADD_COLUMN.value:
                    add_column(table_name)
                    break
                case SC.CHANGE_ORDER.value:
                    change_order(table_name)
                    break
            }
        }
    }

    try {
        prepare()
        process(insert_table)
    } finally {

    }

}