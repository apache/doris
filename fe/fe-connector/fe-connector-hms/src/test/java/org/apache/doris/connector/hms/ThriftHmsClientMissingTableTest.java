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
import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;

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

    @Test
    void partitionNameRpcAbsenceGetsTypedIdentity() throws java.io.IOException {
        for (int poolSize : new int[] {0, 1}) {
            for (boolean scan : new boolean[] {false, true}) {
                for (Exception missing : new Exception[] {
                        new NoSuchObjectException("gone"), new UnknownTableException("gone")}) {
                    try (ThriftHmsClient client = clientThrowing(missing, "listPartitionNames", poolSize)) {
                        HmsClientException wrapped = Assertions.assertThrows(HmsClientException.class,
                                () -> listNames(client, scan));
                        Assertions.assertTrue(wrapped.getCause() instanceof ConnectorTableNotFoundException);
                        ConnectorTableNotFoundException typed = (ConnectorTableNotFoundException) wrapped.getCause();
                        Assertions.assertEquals("db", typed.getDatabaseName());
                        Assertions.assertEquals("tbl", typed.getTableName());
                        Assertions.assertSame(missing, typed.getCause());
                    }
                }
            }
        }
    }

    @Test
    void ordinaryPartitionNameFailuresKeepTheirCause() throws java.io.IOException {
        for (boolean scan : new boolean[] {false, true}) {
            for (Exception failure : new Exception[] {
                    new MetaException("permission denied"), new IllegalStateException("connection failed")}) {
                try (ThriftHmsClient client = clientThrowing(failure, "listPartitionNames", 0)) {
                    HmsClientException wrapped = Assertions.assertThrows(HmsClientException.class,
                            () -> listNames(client, scan));
                    Assertions.assertSame(failure, wrapped.getCause());
                }
            }
        }
    }

    @Test
    void scanCancellationStillPropagatesWithoutAbsenceClassification() throws java.io.IOException {
        CancellationException failure = new CancellationException("cancelled during RPC");
        try (ThriftHmsClient client = clientThrowing(failure, "listPartitionNames", 1)) {
            Assertions.assertSame(failure, Assertions.assertThrows(CancellationException.class,
                    () -> listNames(client, true)));
        }
    }

    @Test
    void missingIndividualPartitionDoesNotBecomeTableAbsence() throws java.io.IOException {
        NoSuchObjectException missing = new NoSuchObjectException("partition is gone");
        try (ThriftHmsClient client = clientThrowing(missing, "getPartition", 0)) {
            HmsClientException wrapped = Assertions.assertThrows(HmsClientException.class,
                    () -> client.getPartition("db", "tbl", Collections.singletonList("1")));
            Assertions.assertSame(missing, wrapped.getCause());
        }
    }

    @Test
    void successfulEmptyPartitionListingIsNotAbsence() throws java.io.IOException {
        for (boolean scan : new boolean[] {false, true}) {
            IMetaStoreClient fake = (IMetaStoreClient) Proxy.newProxyInstance(
                    IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                    (proxy, method, args) -> "listPartitionNames".equals(method.getName())
                            ? Collections.emptyList() : method.getReturnType() == boolean.class ? false : null);
            try (ThriftHmsClient client = new ThriftHmsClient(new HmsClientConfig(Collections.emptyMap(), 0), null,
                    hiveConf -> fake, HmsTypeMapping.Options.DEFAULT)) {
                Assertions.assertEquals(Collections.emptyList(), listNames(client, scan));
            }
        }
    }

    private List<String> listNames(ThriftHmsClient client, boolean scan) {
        return scan ? client.listPartitionNames("db", "tbl", -1, new ConnectorScanContext())
                : client.listPartitionNamesFresh("db", "tbl", -1);
    }

    private ThriftHmsClient clientThrowing(Exception failure) {
        return clientThrowing(failure, "getTable", 0);
    }

    private ThriftHmsClient clientThrowing(Exception failure, String rpc, int poolSize) {
        IMetaStoreClient fake = (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> {
                    if (rpc.equals(method.getName())) {
                        throw failure;
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        return new ThriftHmsClient(new HmsClientConfig(Collections.emptyMap(), poolSize), null,
                hiveConf -> fake, HmsTypeMapping.Options.DEFAULT);
    }
}
