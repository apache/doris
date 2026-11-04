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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class PluginDrivenScanNodeRangeClassLoaderTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rangeCallbacksPinPluginLoaderOnQueryAndWorkerThreads(boolean workerThread) throws Exception {
        assertRangeCallbacks(workerThread, null);
    }

    @ParameterizedTest
    @CsvSource({"false,tableFormat", "false,rangeParams", "false,proof",
            "true,tableFormat", "true,rangeParams", "true,proof"})
    void callbackFailureRestoresCallerLoader(boolean workerThread, String failingCallback) throws Exception {
        assertRangeCallbacks(workerThread, failingCallback);
    }

    private void assertRangeCallbacks(boolean workerThread, String failingCallback) throws Exception {
        try (URLClassLoader pluginLoader = new URLClassLoader(new URL[0],
                ConnectorScanPlanProvider.class.getClassLoader());
                URLClassLoader callerLoader = new URLClassLoader(new URL[0], null)) {
            ConnectorScanPlanProvider provider = (ConnectorScanPlanProvider) Proxy.newProxyInstance(
                    pluginLoader, new Class<?>[] {ConnectorScanPlanProvider.class},
                    (proxy, method, args) -> {
                        throw new UnsupportedOperationException(method.getName());
                    });
            Connector connector = Mockito.mock(Connector.class);
            ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
            Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
            PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
            Deencapsulation.setField(node, "connector", connector);
            Deencapsulation.setField(node, "currentHandle", handle);
            Deencapsulation.setField(node, "conjuncts", Collections.emptyList());
            TFileScanRangeParams params = new TFileScanRangeParams();
            Deencapsulation.setField(node, "params", params);
            ConnectorScanRange scanRange = Mockito.mock(ConnectorScanRange.class);
            Mockito.when(scanRange.getSplitIdentity()).thenReturn("range-7");
            List<String> callbacks = new ArrayList<>();
            IllegalStateException failure = new IllegalStateException("range callback failed");
            Mockito.when(scanRange.getTableFormatType()).thenAnswer(call -> {
                checkCallback("tableFormat", failingCallback, pluginLoader, callbacks, failure);
                return "iceberg";
            });
            Mockito.doAnswer(call -> {
                checkCallback("rangeParams", failingCallback, pluginLoader, callbacks, failure);
                return null;
            }).when(scanRange).populateRangeParams(Mockito.any(), Mockito.any());
            Mockito.when(scanRange.getPartitionEnforcedConjuncts(Mockito.anyMap())).thenAnswer(call -> {
                checkCallback("proof", failingCallback, pluginLoader, callbacks, failure);
                return Collections.emptySet();
            });
            PluginDrivenSplit split = new PluginDrivenSplit(scanRange);
            TFileRangeDesc range = new TFileRangeDesc();
            range.setFormatType(TFileFormatType.FORMAT_PARQUET);
            Runnable convertRange = () -> {
                Thread thread = Thread.currentThread();
                ClassLoader original = thread.getContextClassLoader();
                try {
                    // Batch range conversion happens after the split producer restores its own TCCL.
                    thread.setContextClassLoader(callerLoader);
                    if (failingCallback == null) {
                        node.setScanParams(range, split);
                        Assertions.assertEquals(Arrays.asList("tableFormat", "rangeParams", "proof"), callbacks);
                        Assertions.assertEquals("iceberg", range.getTableFormatParams().getTableFormatType());
                        Assertions.assertTrue(params.isContainsNativeParquet());
                    } else {
                        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                                () -> node.setScanParams(range, split)));
                        Assertions.assertEquals(failingCallback, callbacks.get(callbacks.size() - 1));
                    }
                    Assertions.assertSame(callerLoader, thread.getContextClassLoader());
                } finally {
                    thread.setContextClassLoader(original);
                }
            };
            if (workerThread) {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    executor.submit(convertRange).get(10, TimeUnit.SECONDS);
                } finally {
                    executor.shutdownNow();
                    Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                }
            } else {
                convertRange.run();
            }
        }
    }

    private void checkCallback(String callback, String failingCallback, ClassLoader pluginLoader,
            List<String> callbacks, IllegalStateException failure) {
        callbacks.add(callback);
        Assertions.assertSame(pluginLoader, Thread.currentThread().getContextClassLoader(), callback);
        if (callback.equals(failingCallback)) {
            throw failure;
        }
    }
}
