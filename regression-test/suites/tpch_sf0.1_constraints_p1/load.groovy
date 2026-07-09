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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

// syntax error:
// q06 q13 q15
// Test 23 suites, failed 3 suites

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq
suite("load") {
    def tables = [customer: ["c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment,temp"],
                  lineitem: ["l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp"],
                  nation  : ["n_nationkey, n_name, n_regionkey, n_comment, temp"],
                  orders  : ["o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp"],
                  part    : ["p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp"],
                  partsupp: ["ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,temp"],
                  region  : ["r_regionkey, r_name, r_comment,temp"],
                  supplier: ["s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment,temp"]]

    tables.forEach { tableName, columns ->
        sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName}_delete.sql""").text
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table "${tableName}"

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', "${columns[0]}"

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/tpch/sf0.1/${tableName}.tbl.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def table = "revenue1"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    sql """ sync """

    // Create TPC-H constraints (PK, FK, UK) for query optimization
    log.info("Creating TPC-H constraints ...")

    // Drop existing constraints first to make setup idempotent on rerun.
    // FK constraints must be dropped before their referenced PK/UK constraints.

    // Drop Foreign Key Constraints (idempotent)
    try { sql """ alter table lineitem drop constraint l_ps_fk """ } catch (Exception e) {}
    try { sql """ alter table lineitem drop constraint l_s_fk """ } catch (Exception e) {}
    try { sql """ alter table lineitem drop constraint l_p_fk """ } catch (Exception e) {}
    try { sql """ alter table lineitem drop constraint l_o_fk """ } catch (Exception e) {}
    try { sql """ alter table orders drop constraint o_c_fk """ } catch (Exception e) {}
    try { sql """ alter table partsupp drop constraint ps_s_fk """ } catch (Exception e) {}
    try { sql """ alter table partsupp drop constraint ps_p_fk """ } catch (Exception e) {}
    try { sql """ alter table customer drop constraint c_n_fk """ } catch (Exception e) {}
    try { sql """ alter table supplier drop constraint s_n_fk """ } catch (Exception e) {}
    try { sql """ alter table nation drop constraint n_r_fk """ } catch (Exception e) {}

    // Drop Unique Key Constraints (idempotent)
    try { sql """ alter table region drop constraint r_uk """ } catch (Exception e) {}
    try { sql """ alter table nation drop constraint n_uk """ } catch (Exception e) {}

    // Drop Primary Key Constraints (idempotent)
    try { sql """ alter table lineitem drop constraint l_pk """ } catch (Exception e) {}
    try { sql """ alter table orders drop constraint o_pk """ } catch (Exception e) {}
    try { sql """ alter table partsupp drop constraint ps_pk """ } catch (Exception e) {}
    try { sql """ alter table part drop constraint p_pk """ } catch (Exception e) {}
    try { sql """ alter table customer drop constraint c_pk """ } catch (Exception e) {}
    try { sql """ alter table supplier drop constraint s_pk """ } catch (Exception e) {}
    try { sql """ alter table nation drop constraint n_pk """ } catch (Exception e) {}
    try { sql """ alter table region drop constraint r_pk """ } catch (Exception e) {}

    // Add Primary Key Constraints
    sql """ alter table region add constraint r_pk primary key (r_regionkey) """
    sql """ alter table nation add constraint n_pk primary key (n_nationkey) """
    sql """ alter table supplier add constraint s_pk primary key (s_suppkey) """
    sql """ alter table customer add constraint c_pk primary key (c_custkey) """
    sql """ alter table part add constraint p_pk primary key (p_partkey) """
    sql """ alter table partsupp add constraint ps_pk primary key (ps_partkey, ps_suppkey) """
    sql """ alter table orders add constraint o_pk primary key (o_orderkey) """
    sql """ alter table lineitem add constraint l_pk primary key (l_orderkey, l_linenumber) """

    // Add Foreign Key Constraints
    sql """ alter table nation add constraint n_r_fk foreign key (n_regionkey) references region(r_regionkey) """
    sql """ alter table supplier add constraint s_n_fk foreign key (s_nationkey) references nation(n_nationkey) """
    sql """ alter table customer add constraint c_n_fk foreign key (c_nationkey) references nation(n_nationkey) """
    sql """ alter table partsupp add constraint ps_p_fk foreign key (ps_partkey) references part(p_partkey) """
    sql """ alter table partsupp add constraint ps_s_fk foreign key (ps_suppkey) references supplier(s_suppkey) """
    sql """ alter table orders add constraint o_c_fk foreign key (o_custkey) references customer(c_custkey) """
    sql """ alter table lineitem add constraint l_o_fk foreign key (l_orderkey) references orders(o_orderkey) """
    sql """ alter table lineitem add constraint l_p_fk foreign key (l_partkey) references part(p_partkey) """
    sql """ alter table lineitem add constraint l_s_fk foreign key (l_suppkey) references supplier(s_suppkey) """
    sql """ alter table lineitem add constraint l_ps_fk foreign key (l_partkey, l_suppkey) references partsupp(ps_partkey, ps_suppkey) """

    // Add Unique Key Constraints
    sql """ alter table region add constraint r_uk unique (r_name) """
    sql """ alter table nation add constraint n_uk unique (n_name) """

    log.info("TPC-H constraints created successfully.")
}
