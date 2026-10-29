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

import org.apache.doris.connector.spi.ConnectorTableNotFoundException;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;

class ThriftHmsClientMissingTableTest {
    @Test
    void onlyTableRpcAbsenceGetsTypedIdentity() throws java.io.IOException {
        for (Exception missing : new Exception[] {
                new NoSuchObjectException("gone"), new UnknownTableException("gone")}) {
            try (ThriftHmsClient client = clientThrowing(missing)) {
                HmsClientException wrapped = Assertions.assertThrows(
                        HmsClientException.class, () -> client.getTableFresh("db", "tbl"));
                Assertions.assertTrue(wrapped.getCause() instanceof ConnectorTableNotFoundException);
                ConnectorTableNotFoundException failure = (ConnectorTableNotFoundException) wrapped.getCause();
                Assertions.assertEquals("db", failure.getDatabaseName());
                Assertions.assertEquals("tbl", failure.getTableName());
                Assertions.assertSame(missing, failure.getCause());
            }
        }
    }

    @Test
    void ordinaryReadFailuresKeepTheirOriginalContract() throws java.io.IOException {
        try (ThriftHmsClient client = clientThrowing(new IllegalStateException("connection failed"))) {
            Assertions.assertThrows(HmsClientException.class, () -> client.getTableFresh("db", "tbl"));
        }
    }

    private ThriftHmsClient clientThrowing(Exception failure) {
        IMetaStoreClient fake = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    if ("getTable".equals(method.getName())) {
                        throw failure;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        return new ThriftHmsClient(new HmsClientConfig(Collections.emptyMap(), 0), null,
                hiveConf -> fake, HmsTypeMapping.Options.DEFAULT);
    }
}
