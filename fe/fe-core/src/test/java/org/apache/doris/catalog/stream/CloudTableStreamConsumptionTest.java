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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.thrift.TRow;
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
    }

    @Test
    public void testConsumptionViewReadsAuthoritativeCloudOffsets() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("test_cloud_stream_consumption");
        OlapTable table = (OlapTable) db.getTableOrMetaException("base_table");
        long p1 = table.getPartition("p1").getId();
        long p2 = table.getPartition("p2").getId();

        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        Config.cloud_unique_id = "cloud_table_stream_ut";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.getTableStreamReadState(Mockito.any())).thenAnswer(invocation -> {
                Cloud.GetTableStreamReadStateRequest request = invocation.getArgument(0);
                Assertions.assertEquals(1, request.getBindingsCount());
                Assertions.assertEquals(Set.of(p1, p2),
                        new HashSet<>(request.getBindings(0).getPartitionIdsList()));
                return Cloud.GetTableStreamReadStateResponse.newBuilder()
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
                                        .setLastConsumptionTimeMs(999))
                                .addPartitionStates(Cloud.TableStreamPartitionReadStatePB.newBuilder()
                                        .setPartitionId(p2)
                                        .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                                        .setEndTso(200)
                                        .setVisibleVersion(1)))
                        .build();
            });

            List<TRow> rows = new ArrayList<>();
            Env.getCurrentEnv().getTableStreamManager().fillStreamConsumptionValuesMetadataResult(rows);
            rows.sort(Comparator.comparing(row -> row.getColumnValue().get(3).getStringVal()));
            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals("p1", rows.get(0).getColumnValue().get(3).getStringVal());
            Assertions.assertEquals("100", rows.get(0).getColumnValue().get(4).getStringVal());
            Assertions.assertEquals("30", rows.get(0).getColumnValue().get(5).getStringVal());
            Assertions.assertEquals(999, rows.get(0).getColumnValue().get(6).getLongVal());
            Assertions.assertEquals("p2", rows.get(1).getColumnValue().get(3).getStringVal());
            Assertions.assertEquals("N/A", rows.get(1).getColumnValue().get(4).getStringVal());
            Assertions.assertEquals("0", rows.get(1).getColumnValue().get(5).getStringVal());
            Assertions.assertEquals(-1, rows.get(1).getColumnValue().get(6).getLongVal());
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }
}
