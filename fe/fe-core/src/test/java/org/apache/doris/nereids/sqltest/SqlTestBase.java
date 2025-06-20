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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.LogicalCompatibilityContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import java.util.BitSet;

public abstract class SqlTestBase extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        // make table id is larger than Integer.MAX_VALUE
        IdGeneratorBuffer idGeneratorBuffer =
                Env.getCurrentEnv().getIdGeneratorBuffer(Integer.MAX_VALUE + 10L);
        idGeneratorBuffer.getNextId();

        createTables(
                "CREATE TABLE IF NOT EXISTS T0 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T4 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "AUTO PARTITION BY LIST(`id`)\n"
                        + "(\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS orders  (\n"
                        + "      o_orderkey       INTEGER NOT NULL,\n"
                        + "      o_custkey        INTEGER NOT NULL,\n"
                        + "      o_orderstatus    CHAR(1) NOT NULL,\n"
                        + "      o_totalprice     DECIMALV3(15,2) NOT NULL,\n"
                        + "      o_orderdate      DATE NOT NULL,\n"
                        + "      o_orderpriority  CHAR(15) NOT NULL,  \n"
                        + "      o_clerk          CHAR(15) NOT NULL, \n"
                        + "      o_shippriority   INTEGER NOT NULL,\n"
                        + "      O_COMMENT        VARCHAR(79) NOT NULL\n"
                        + "    )\n"
                        + "    DUPLICATE KEY(o_orderkey, o_custkey)\n"
                        + "    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3\n"
                        + "    PROPERTIES (\n"
                        + "      \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE TABLE IF NOT EXISTS lineitem (\n"
                        + "      l_orderkey    INTEGER NOT NULL,\n"
                        + "      l_partkey     INTEGER NOT NULL,\n"
                        + "      l_suppkey     INTEGER NOT NULL,\n"
                        + "      l_linenumber  INTEGER NOT NULL,\n"
                        + "      l_quantity    DECIMALV3(15,2) NOT NULL,\n"
                        + "      l_extendedprice  DECIMALV3(15,2) NOT NULL,\n"
                        + "      l_discount    DECIMALV3(15,2) NOT NULL,\n"
                        + "      l_tax         DECIMALV3(15,2) NOT NULL,\n"
                        + "      l_returnflag  CHAR(1) NOT NULL,\n"
                        + "      l_linestatus  CHAR(1) NOT NULL,\n"
                        + "      l_shipdate    DATE NOT NULL,\n"
                        + "      l_commitdate  DATE NOT NULL,\n"
                        + "      l_receiptdate DATE NOT NULL,\n"
                        + "      l_shipinstruct CHAR(25) NOT NULL,\n"
                        + "      l_shipmode     CHAR(10) NOT NULL,\n"
                        + "      l_comment      VARCHAR(44) NOT NULL\n"
                        + "    )\n"
                        + "    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)\n"
                        + "    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3\n"
                        + "    PROPERTIES (\n"
                        + "      \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE TABLE IF NOT EXISTS partsupp (\n"
                        + "      ps_partkey     INTEGER NOT NULL,\n"
                        + "      ps_suppkey     INTEGER NOT NULL,\n"
                        + "      ps_availqty    INTEGER NOT NULL,\n"
                        + "      ps_supplycost  DECIMALV3(15,2)  NOT NULL,\n"
                        + "      ps_comment     VARCHAR(199) NOT NULL \n"
                        + "    )\n"
                        + "    DUPLICATE KEY(ps_partkey, ps_suppkey)\n"
                        + "    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3\n"
                        + "    PROPERTIES (\n"
                        + "      \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE TABLE IF NOT EXISTS part (\n"
                        + "  P_PARTKEY     INTEGER NOT NULL,\n"
                        + "  P_NAME        VARCHAR(55) NOT NULL,\n"
                        + "  P_MFGR        CHAR(25) NOT NULL,\n"
                        + "  P_BRAND       CHAR(10) NOT NULL,\n"
                        + "  P_TYPE        VARCHAR(25) NOT NULL,\n"
                        + "  P_SIZE        INTEGER NOT NULL,\n"
                        + "  P_CONTAINER   CHAR(10) NOT NULL,\n"
                        + "  P_RETAILPRICE DECIMAL(15,2) NOT NULL,\n"
                        + "  P_COMMENT     VARCHAR(23) NOT NULL \n"
                        + ")\n"
                        + "DUPLICATE KEY(P_PARTKEY, P_NAME)\n"
                        + "DISTRIBUTED BY HASH(P_PARTKEY) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")",
                "CREATE TABLE customer (\n"
                        + "        c_custkey     int NOT NULL,\n"
                        + "        c_name        VARCHAR(25) NOT NULL,\n"
                        + "        c_address     VARCHAR(40) NOT NULL,\n"
                        + "        c_nationkey   int NOT NULL,\n"
                        + "        c_phone       VARCHAR(15) NOT NULL,\n"
                        + "        c_acctbal     decimal(15, 2)   NOT NULL,\n"
                        + "        c_mktsegment  VARCHAR(10) NOT NULL,\n"
                        + "        c_comment     VARCHAR(117) NOT NULL\n"
                        + "    )ENGINE=OLAP\n"
                        + "    DUPLICATE KEY(`c_custkey`)\n"
                        + "    COMMENT \"OLAP\"\n"
                        + "    DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24\n"
                        + "    PROPERTIES (\n"
                        + "        \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE TABLE supplier (\n"
                        + "        s_suppkey       int NOT NULL,\n"
                        + "        s_name        VARCHAR(25) NOT NULL,\n"
                        + "        s_address     VARCHAR(40) NOT NULL,\n"
                        + "        s_nationkey   int NOT NULL,\n"
                        + "        s_phone       VARCHAR(15) NOT NULL,\n"
                        + "        s_acctbal     decimal(15, 2) NOT NULL,\n"
                        + "        s_comment     VARCHAR(101) NOT NULL\n"
                        + "    )ENGINE=OLAP\n"
                        + "    DUPLICATE KEY(`s_suppkey`)\n"
                        + "    COMMENT \"OLAP\"\n"
                        + "    DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n"
                        + "    PROPERTIES (\n"
                        + "        \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE TABLE `nation` (\n"
                        + "    `n_nationkey` int(11) NOT NULL,\n"
                        + "    `n_name`      varchar(25) NOT NULL,\n"
                        + "    `n_regionkey` int(11) NOT NULL,\n"
                        + "    `n_comment`   varchar(152) NULL\n"
                        + "    ) ENGINE=OLAP\n"
                        + "    DUPLICATE KEY(`N_NATIONKEY`)\n"
                        + "    COMMENT \"OLAP\"\n"
                        + "    DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n"
                        + "    PROPERTIES (\n"
                        + "        \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE TABLE region  (\n"
                        + "        r_regionkey      int NOT NULL,\n"
                        + "        r_name       VARCHAR(25) NOT NULL,\n"
                        + "        r_comment    VARCHAR(152)\n"
                        + "    )ENGINE=OLAP\n"
                        + "    DUPLICATE KEY(`r_regionkey`)\n"
                        + "    COMMENT \"OLAP\"\n"
                        + "    DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n"
                        + "    PROPERTIES (\n"
                        + "        \"replication_num\" = \"1\"\n"
                        + "    );",
                "CREATE VIEW IF NOT EXISTS test.revenue1 AS\n"
                        + "  SELECT\n"
                        + "    l_suppkey AS supplier_no,\n"
                        + "    sum(l_extendedprice * (1 - l_discount)) AS total_revenue\n"
                        + "  FROM\n"
                        + "    lineitem\n"
                        + "  WHERE\n"
                        + "    l_shipdate >= DATE '1996-01-01'\n"
                        + "    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH\n"
                        + "GROUP BY\n"
                        + "  l_suppkey;"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    protected LogicalCompatibilityContext constructContext(Plan p1, Plan p2, CascadesContext context) {
        StructInfo st1 = MaterializedViewUtils.extractStructInfo(p1, p1,
                context, new BitSet()).get(0);
        StructInfo st2 = MaterializedViewUtils.extractStructInfo(p2, p2,
                context, new BitSet()).get(0);
        RelationMapping rm = RelationMapping.generate(st1.getRelations(), st2.getRelations(), 8)
                .get(0);
        SlotMapping sm = SlotMapping.generate(rm);
        return LogicalCompatibilityContext.from(rm, sm, st1, st2);
    }
}
