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

package org.apache.doris.planner;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class RemoteOlapTableSinkTest {
    @Test
    void onlyRemoteSinksSelectTheExternalEndpointPolicy() {
        RemoteOlapTable table = Mockito.mock(RemoteOlapTable.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(table.getCatalog().getFeServiceClient().getMasterAddress())
                .thenReturn(new TNetworkAddress("remote.example", 9020));
        RemoteOlapTableSink remote = new RemoteOlapTableSink(table, null, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyMap());
        remote.tDataSink = sink();
        TOlapTableSink serialized = remote.toThrift().getOlapTableSink();
        Assertions.assertTrue(serialized.isRemoteEndpoint());
        Assertions.assertEquals("remote.example", serialized.getPartition().getMasterAddress().hostname);
        OlapTableSink local = new OlapTableSink(Mockito.mock(OlapTable.class), null, Collections.emptyList());
        local.tDataSink = sink();
        Assertions.assertFalse(local.toThrift().getOlapTableSink().isRemoteEndpoint());
        Assertions.assertFalse(local.toThrift().getOlapTableSink().isSetRemoteEndpoint());
    }

    private TDataSink sink() {
        return new TDataSink(TDataSinkType.OLAP_TABLE_SINK).setOlapTableSink(
                new TOlapTableSink().setPartition(new TOlapTablePartitionParam()));
    }
}
