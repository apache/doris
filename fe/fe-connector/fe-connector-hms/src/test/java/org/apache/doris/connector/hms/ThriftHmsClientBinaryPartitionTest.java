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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;

class ThriftHmsClientBinaryPartitionTest {
    @Test
    void binaryPartitionKeyKeepsTextWhileDataColumnKeepsBytes() {
        Table remote = new Table();
        remote.setDbName("db");
        remote.setTableName("tbl");
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(Collections.singletonList(new FieldSchema("payload", "binary", "")));
        remote.setSd(sd);
        remote.setPartitionKeys(Collections.singletonList(new FieldSchema("part_key", "binary", "")));
        IMetaStoreClient fake = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    if ("getTable".equals(method.getName())) {
                        return remote;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        ThriftHmsClient client = new ThriftHmsClient(new HmsClientConfig(Collections.emptyMap(), 0), null,
                hiveConf -> fake, HmsTypeMapping.Options.DEFAULT);

        HmsTableInfo table = client.getTable("db", "tbl");
        // Partition values come from directory names, so existing partitions must keep their text contract.
        Assertions.assertEquals("STRING", table.getPartitionKeys().get(0).getType().getTypeName());
        Assertions.assertEquals("VARBINARY", table.getColumns().get(0).getType().getTypeName());
    }
}
