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

public class ThriftHmsClientTableConversionTest {

    @Test
    public void testGetTablePreservesNativeBinaryPartitionType() throws Exception {
        Table table = new Table();
        table.setDbName("db");
        table.setTableName("tbl");
        table.setSd(new StorageDescriptor());
        table.setPartitionKeys(Collections.singletonList(new FieldSchema("code", "binary", null)));
        IMetaStoreClient metastore = (IMetaStoreClient) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {IMetaStoreClient.class}, (proxy, method, args) -> {
                    if ("getTable".equals(method.getName())) {
                        return table;
                    }
                    return null;
                });
        ThriftHmsClient client = new ThriftHmsClient(new HmsClientConfig(Collections.emptyMap(), 0),
                new ThriftHmsClient.AuthAction() {
                    @Override
                    public <T> T execute(java.util.concurrent.Callable<T> callable) throws Exception {
                        return callable.call();
                    }
                }, hiveConf -> metastore, HmsTypeMapping.Options.DEFAULT);

        try {
            HmsTableInfo tableInfo = client.getTable("db", "tbl");
            Assertions.assertEquals("STRING", tableInfo.getPartitionKeys().get(0).getType().getTypeName());
            Assertions.assertEquals("binary", tableInfo.getPartitionKeyHiveTypes().get("code"));
        } finally {
            client.close();
        }
    }
}
