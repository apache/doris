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

package org.apache.doris.transaction;

import org.apache.doris.catalog.TableProperty;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.transaction.TxnUtil;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SequenceRetentionTest {
    @Test
    public void testGapParsingAndOverflowBoundaries() throws Exception {
        Assertions.assertEquals(Long.MAX_VALUE,
                PropertyAnalyzer.parseSequenceRetentionGap(Long.toString(Long.MAX_VALUE)));
        Assertions.assertThrows(AnalysisException.class, () -> PropertyAnalyzer.parseSequenceRetentionGap("-1"));
        Assertions.assertThrows(AnalysisException.class,
                () -> PropertyAnalyzer.parseSequenceRetentionGap("9223372036854775808"));
        TransactionState transaction = new TransactionState();
        transaction.recordSequenceRetentionVersions(11, Map.of(22L, 1L));
        transaction.checkSequenceRetentionCommit(11, 22, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void testImmutableGapAndJournalReplay() throws Exception {
        TableProperty property = new TableProperty(Map.of(PropertyAnalyzer.PROPERTIES_SEQUENCE_RETENTION_GAP, "100"));
        TableProperty replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(property), TableProperty.class);
        TransactionState transaction = new TransactionState();
        transaction.recordSequenceRetentionVersions(11, Map.of(22L, 620L, 23L, 651L));
        TransactionState recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(transaction), TransactionState.class);
        Assertions.assertEquals(100, replayed.getSequenceRetentionGap());
        Assertions.assertEquals(620, recovered.getSequenceRetentionPlanVersion(11, 22));
        Assertions.assertEquals(651, recovered.getSequenceRetentionPlanVersion(11, 23));
        recovered.checkSequenceRetentionCommit(11, 22, replayed.getSequenceRetentionGap(), 720);
        Assertions.assertThrows(UserException.class, () -> recovered.checkSequenceRetentionCommit(11, 22, 100, 721));
        Assertions.assertThrows(UserException.class, () -> recovered.checkSequenceRetentionCommit(11, 22, 100, 620));
    }

    @Test
    public void testCloudPlanningVersionsRecoverAcrossTables() throws Exception {
        Cloud.TxnInfoPB stored = Cloud.TxnInfoPB.newBuilder().setDbId(1).setTxnId(12).addTableIds(11).addTableIds(12)
                .setStatus(Cloud.TxnStatusPB.TXN_STATUS_PREPARED)
                .addSequenceRetentionPlans(Cloud.SequenceRetentionPlanPB.newBuilder()
                        .setTableId(11).setVersionGap(100).putPartitionVersions(22, 620))
                .addSequenceRetentionPlans(Cloud.SequenceRetentionPlanPB.newBuilder()
                        .setTableId(12).setVersionGap(20).putPartitionVersions(23, 700)).build();
        TransactionState recovered = TxnUtil.transactionStateFromPb(stored);
        recovered.checkSequenceRetentionCommit(11, 22, 100, 720);
        recovered.checkSequenceRetentionCommit(12, 23, 20, 720);
        Assertions.assertThrows(UserException.class, () -> recovered.checkSequenceRetentionCommit(11, 23, 100, 722));
        recovered.recordSequenceRetentionVersions(11, Map.of(22L, 620L));
        Assertions.assertThrows(IllegalStateException.class,
                () -> recovered.recordSequenceRetentionVersions(11, Map.of(22L, 621L)));
        Assertions.assertEquals(620, recovered.getSequenceRetentionPlanVersion(11, 22));
    }
}
