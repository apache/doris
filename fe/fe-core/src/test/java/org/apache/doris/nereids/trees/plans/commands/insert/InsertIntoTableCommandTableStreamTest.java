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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.stream.AbstractTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

public class InsertIntoTableCommandTableStreamTest extends TestWithFeService {

    private final NereidsParser parser = new NereidsParser();

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;

        createDatabase("test_stream");
        connectContext.setDatabase("test_stream");

        String createBaseTable = "create table test_stream.tbl_stream_base (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createBaseTable);

        String createTargetTable = "create table test_stream.tbl_target (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")";
        createTable(createTargetTable);

        String createStream = "create stream if not exists test_stream.s1 on table test_stream.tbl_stream_base\n"
                + "properties('type' = 'default', 'show_initial_rows' = 'true')";
        createTable(createStream);
    }

    @Test
    public void testInitPlanCollectsStreamUpdateInfosForHistoricalConsume() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");

        String sql = "insert into test_stream.tbl_target select * from test_stream.s1";
        LogicalPlan logicalPlan = parser.parseSingle(sql);
        Assertions.assertTrue(logicalPlan instanceof InsertIntoTableCommand);

        connectContext.setStartTime();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        InsertIntoTableCommand command = (InsertIntoTableCommand) logicalPlan;
        AbstractInsertExecutor insertExecutor = command.initPlan(connectContext, executor, true);

        List<TableStreamUpdateInfo> streamUpdateInfos = insertExecutor.getStreamUpdateInfos();
        Assertions.assertNotNull(streamUpdateInfos);
        Assertions.assertEquals(1, streamUpdateInfos.size());

        TableStreamUpdateInfo info = streamUpdateInfos.get(0);
        Assertions.assertEquals(db.getId(), info.getDbId());
        Assertions.assertEquals(stream.getId(), info.getStreamId());

        Assertions.assertTrue(info.getUpdate() instanceof OlapTableStreamUpdate);
        OlapTableStreamUpdate update = (OlapTableStreamUpdate) info.getUpdate();
        Assertions.assertTrue(update.getPrev().isEmpty());

        List<AbstractInsertExecutor.InsertExecutorListener> listeners = Deencapsulation.getField(insertExecutor,
                "listeners");
        for (AbstractInsertExecutor.InsertExecutorListener listener : listeners) {
            listener.beforeComplete(insertExecutor, executor, -1);
        }

        TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(),
                insertExecutor.getTxnId());
        Assertions.assertNotNull(txnState);
        Assertions.assertNotNull(txnState.getStreamUpdateInfos());
        Assertions.assertEquals(1, txnState.getStreamUpdateInfos().size());

        TableStreamUpdateInfo txnInfo = txnState.getStreamUpdateInfos().get(0);
        Assertions.assertEquals(info.getDbId(), txnInfo.getDbId());
        Assertions.assertEquals(info.getStreamId(), txnInfo.getStreamId());
        Assertions.assertTrue(txnInfo.getUpdate() instanceof OlapTableStreamUpdate);

        AbstractTableStreamUpdate txnUpdate = txnInfo.getUpdate();
        Assertions.assertEquals(update.getNext(), ((OlapTableStreamUpdate) txnUpdate).getNext());
    }
}
