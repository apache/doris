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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.common.Config;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.thrift.TDataPartition;
import org.apache.doris.thrift.TExternalTableSinkHashAlgorithm;
import org.apache.doris.thrift.TExternalTableSinkWriterAssignment;
import org.apache.doris.thrift.TPartitionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

class PhysicalPlanTranslatorHiveWriteCompatibilityTest {

    @Test
    void usesBranch41ExternalRoutingUntilAllBackendsSupportAlignedValues() {
        List<Expr> partitionExprs = Collections.singletonList(new IntLiteral(1));

        DataPartition partition = PhysicalPlanTranslator.createHiveTableSinkDataPartition(
                partitionExprs, Config.ALIGNED_EXTERNAL_TABLE_SINK_THRIFT_MIN_BE_EXEC_VERSION - 1);
        TDataPartition thrift = partition.toThrift();

        Assertions.assertEquals(TPartitionType.EXTERNAL_TABLE_SINK_HASH_PARTITIONED, thrift.type);
        Assertions.assertEquals(TExternalTableSinkHashAlgorithm.DIRECT_HASH,
                thrift.external_table_sink_hash_partition_info.algorithm);
        Assertions.assertEquals(TExternalTableSinkWriterAssignment.SKEWED,
                thrift.external_table_sink_hash_partition_info.writer_assignment);
        Assertions.assertEquals("direct_hash",
                thrift.external_table_sink_hash_partition_info.partition_function);
    }

    @Test
    void usesAlignedHiveRoutingAfterRollingUpgrade() {
        List<Expr> partitionExprs = Collections.singletonList(new IntLiteral(1));

        DataPartition partition = PhysicalPlanTranslator.createHiveTableSinkDataPartition(
                partitionExprs, Config.ALIGNED_EXTERNAL_TABLE_SINK_THRIFT_MIN_BE_EXEC_VERSION);

        Assertions.assertEquals(TPartitionType.HIVE_TABLE_SINK_HASH_PARTITIONED,
                partition.toThrift().type);
    }

    @Test
    void preservesUnpartitionedHiveRoutingAcrossRollingUpgrade() {
        DataPartition legacy = PhysicalPlanTranslator.createHiveTableSinkDataPartition(
                Collections.emptyList(),
                Config.ALIGNED_EXTERNAL_TABLE_SINK_THRIFT_MIN_BE_EXEC_VERSION - 1);
        DataPartition aligned = PhysicalPlanTranslator.createHiveTableSinkDataPartition(
                Collections.emptyList(),
                Config.ALIGNED_EXTERNAL_TABLE_SINK_THRIFT_MIN_BE_EXEC_VERSION);

        Assertions.assertEquals(TPartitionType.EXTERNAL_TABLE_SINK_UNPARTITIONED,
                legacy.toThrift().type);
        Assertions.assertEquals(TPartitionType.HIVE_TABLE_SINK_UNPARTITIONED,
                aligned.toThrift().type);
    }

    @Test
    void usesStatementVersionSnapshotWhenGlobalVersionChanges() {
        int originalBeExecVersion = Config.be_exec_version;
        try {
            PlanTranslatorContext context = new PlanTranslatorContext(
                    Config.ALIGNED_EXTERNAL_TABLE_SINK_THRIFT_MIN_BE_EXEC_VERSION);

            Config.be_exec_version = Config.ALIGNED_EXTERNAL_TABLE_SINK_THRIFT_MIN_BE_EXEC_VERSION - 1;
            DataPartition partition = PhysicalPlanTranslator.createHiveTableSinkDataPartition(
                    Collections.emptyList(), context.getBeExecVersion());

            Assertions.assertEquals(TPartitionType.HIVE_TABLE_SINK_UNPARTITIONED,
                    partition.toThrift().type);
        } finally {
            Config.be_exec_version = originalBeExecVersion;
        }
    }
}
