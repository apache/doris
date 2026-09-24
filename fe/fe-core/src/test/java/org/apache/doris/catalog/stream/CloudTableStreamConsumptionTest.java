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

package org.apache.doris.catalog.stream;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.tablefunction.MetadataGenerator;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudTableStreamConsumptionTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        createDatabase("test_cloud_stream_consumption");
        connectContext.setDatabase("test_cloud_stream_consumption");
        createTable("create table test_cloud_stream_consumption.base_table (k1 int, k2 int) "
                + "unique key(k1) partition by range(k1) "
                + "(partition p1 values less than (100), partition p2 values less than (200)) "
                + "distributed by hash(k1) buckets 1 properties("
                + "'replication_num'='1','binlog.enable'='true','binlog.format'='ROW',"
                + "'binlog.need_historical_value'='true')");
        createTable("create stream test_cloud_stream_consumption.s1 "
                + "on table test_cloud_stream_consumption.base_table "
                + "properties('show_initial_rows'='true')");
        createTable("create stream test_cloud_stream_consumption.s2 "
                + "on table test_cloud_stream_consumption.base_table "
                + "properties('show_initial_rows'='true')");
        createTable("create table test_cloud_stream_consumption.empty_base_table (k1 int, k2 int) "
                + "unique key(k1) partition by range(k1) "
                + "(partition p1 values less than (100)) "
                + "distributed by hash(k1) buckets 1 properties("
                + "'replication_num'='1','binlog.enable'='true','binlog.format'='ROW',"
                + "'binlog.need_historical_value'='true')");
        createTable("create stream test_cloud_stream_consumption.empty_stream "
                + "on table test_cloud_stream_consumption.empty_base_table "
                + "properties('show_initial_rows'='true')");
        Database db = (Database) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("test_cloud_stream_consumption");
        OlapTable emptyBaseTable = (OlapTable) db.getTableOrMetaException("empty_base_table");
        emptyBaseTable.writeLock();
        try {
            emptyBaseTable.dropPartitionAndReserveTablet("p1");
        } finally {
            emptyBaseTable.writeUnlock();
        }
    }

    @Test
    public void testConsumptionViewReadsAuthoritativeCloudOffsets() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("test_cloud_stream_consumption");
        OlapTable table = (OlapTable) db.getTableOrMetaException("base_table");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("s1");
        OlapTableStream secondStream = (OlapTableStream) db.getTableOrMetaException("s2");
        long p1 = table.getPartition("p1").getId();
        long p2 = table.getPartition("p2").getId();

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        Config.cloud_unique_id = "cloud_table_stream_ut";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.getTableStreamOffset(Mockito.any())).thenAnswer(invocation -> {
                Cloud.GetTableStreamOffsetRequest request = invocation.getArgument(0);
                if (request.getBindingsCount() == 2) {
                    Assertions.assertEquals(Set.of(stream.getId(), secondStream.getId()),
                            request.getBindingsList().stream()
                                    .map(binding -> binding.getIdentity().getStreamId())
                                    .collect(java.util.stream.Collectors.toSet()));
                    Cloud.GetTableStreamOffsetResponse.Builder response = Cloud.GetTableStreamOffsetResponse
                            .newBuilder().setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                    .setCode(Cloud.MetaServiceCode.OK));
                    request.getBindingsList().forEach(binding -> {
                        Assertions.assertEquals(Set.of(p1, p2), new HashSet<>(binding.getPartitionIdsList()));
                        Cloud.TableStreamReadBindingResultPB.Builder bindingResult =
                                Cloud.TableStreamReadBindingResultPB.newBuilder()
                                        .setIdentity(binding.getIdentity());
                        binding.getPartitionIdsList().forEach(partitionId -> bindingResult.addPartitionStates(
                                Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                        .setPartitionId(partitionId)
                                        .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                                        .setEndTso(200)
                                        .setVisibleVersion(1)));
                        response.addBindings(bindingResult);
                    });
                    return response.build();
                }
                Assertions.assertEquals(1, request.getBindingsCount());
                if (request.getBindings(0).getIdentity().getStreamId() == secondStream.getId()) {
                    Assertions.assertEquals(Set.of(p1, p2),
                            new HashSet<>(request.getBindings(0).getPartitionIdsList()));
                    return Cloud.GetTableStreamOffsetResponse.newBuilder()
                            .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                    .setCode(Cloud.MetaServiceCode.OK))
                            .addBindings(Cloud.TableStreamReadBindingResultPB.newBuilder()
                                    .setIdentity(request.getBindings(0).getIdentity())
                                    .addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                            .setPartitionId(p1)
                                            .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                                            .setEndTso(200)
                                            .setVisibleVersion(1))
                                    .addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                            .setPartitionId(p2)
                                            .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                                            .setEndTso(200)
                                            .setVisibleVersion(1)))
                            .build();
                }
                Assertions.assertEquals(stream.getId(), request.getBindings(0).getIdentity().getStreamId());
                Assertions.assertEquals(Set.of(p1),
                        new HashSet<>(request.getBindings(0).getPartitionIdsList()));
                return Cloud.GetTableStreamOffsetResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK))
                        .addBindings(Cloud.TableStreamReadBindingResultPB.newBuilder()
                                .setIdentity(request.getBindings(0).getIdentity())
                                .addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                        .setPartitionId(p1)
                                        .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                                        .setOffsetTso(100)
                                        .setEndTso(130)
                                        .setVisibleVersion(8)
                                        .setLastConsumptionTimeMs(999)))
                                .build();
            });

            MonitoredReentrantReadWriteLock tableLock = Deencapsulation.getField(table, "rwLock");
            MonitoredReentrantReadWriteLock streamLock = Deencapsulation.getField(stream, "rwLock");
            AtomicBoolean unitSelected = new AtomicBoolean(false);
            List<TRow> cloudRows = new ArrayList<>();
            Env.getCurrentEnv().getTableStreamManager().fillStreamConsumptionValuesMetadataResult(
                    cloudRows, (dbName, streamName, streamId, unit) -> {
                        if (unit != null) {
                            unitSelected.set(true);
                            Assertions.assertEquals(0, streamLock.getReadHoldCount());
                            Assertions.assertEquals(0, tableLock.getReadHoldCount());
                        }
                        return streamName.equals("s1") && (unit == null || unit.equals("p1"));
                    });
            Assertions.assertTrue(unitSelected.get());
            Assertions.assertEquals(1, cloudRows.size());
            Assertions.assertEquals("p1", cloudRows.get(0).getColumnValue().get(3).getStringVal());

            TFetchSchemaTableDataRequest request = newConsumptionRequest("test_cloud_stream_consumption", "s1");
            TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(request);
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            List<TRow> rows = new ArrayList<>(result.getDataBatch());
            rows.sort(Comparator.comparing(row -> row.getColumnValue().get(3).getStringVal()));
            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals("p1", rows.get(0).getColumnValue().get(3).getStringVal());
            Assertions.assertEquals("100", rows.get(0).getColumnValue().get(4).getStringVal());
            Assertions.assertEquals("30", rows.get(0).getColumnValue().get(5).getStringVal());
            Assertions.assertEquals(999, rows.get(0).getColumnValue().get(6).getLongVal());

            result = MetadataGenerator.getSchemaTableData(newStreamIdConsumptionRequest(secondStream.getId()));
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Assertions.assertEquals(2, result.getDataBatchSize());
            Assertions.assertTrue(result.getDataBatch().stream()
                    .allMatch(row -> row.getColumnValue().get(2).getLongVal() == secondStream.getId()));

            TSchemaTableRequestParams invalidParams = new TSchemaTableRequestParams();
            invalidParams.setFrontendConjuncts("{");
            result = MetadataGenerator.getSchemaTableData(new TFetchSchemaTableDataRequest()
                    .setSchemaTableName(TSchemaTableName.TABLE_STREAM_CONSUMPTION)
                    .setSchemaTableParams(invalidParams));
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Assertions.assertEquals(4, result.getDataBatchSize());

            table.writeLock();
            try {
                table.dropPartitionAndReserveTablet("p1");
                table.dropPartitionAndReserveTablet("p2");
            } finally {
                table.writeUnlock();
            }
            result = MetadataGenerator.getSchemaTableData(request);
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Assertions.assertTrue(result.getDataBatch().isEmpty());
            Mockito.verify(proxy, Mockito.times(4)).getTableStreamOffset(Mockito.any());
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }

    private TFetchSchemaTableDataRequest newConsumptionRequest(String dbName, String streamName) {
        TSchemaTableRequestParams params = new TSchemaTableRequestParams();
        params.setFrontendConjuncts(GsonUtils.GSON.toJson(List.of(
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "DB_NAME"), new StringLiteral(dbName)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "STREAM_NAME"), new StringLiteral(streamName)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "UNIT"), new StringLiteral("p1")),
                new BinaryPredicate(BinaryPredicate.Operator.GT,
                        new SlotRef(null, "LAG"), new IntLiteral(100)))));
        return new TFetchSchemaTableDataRequest()
                .setSchemaTableName(TSchemaTableName.TABLE_STREAM_CONSUMPTION)
                .setSchemaTableParams(params);
    }

    private TFetchSchemaTableDataRequest newStreamIdConsumptionRequest(long streamId) {
        TSchemaTableRequestParams params = new TSchemaTableRequestParams();
        params.setFrontendConjuncts(GsonUtils.GSON.toJson(List.of(
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "STREAM_ID"), new IntLiteral(streamId)))));
        return new TFetchSchemaTableDataRequest()
                .setSchemaTableName(TSchemaTableName.TABLE_STREAM_CONSUMPTION)
                .setSchemaTableParams(params);
    }
}
