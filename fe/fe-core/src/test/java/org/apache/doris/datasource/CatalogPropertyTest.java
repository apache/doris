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

package org.apache.doris.datasource;

import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CatalogPropertyTest {

    @Test
    public void testHadoopPropertiesArePublishedAfterInitialization() throws Exception {
        CountDownLatch iterationStarted = new CountDownLatch(1);
        CountDownLatch allowIteration = new CountDownLatch(1);
        Configuration configuration = new BlockingConfiguration(iterationStarted, allowIteration);
        configuration.set("fs.test.property", "complete");

        StorageProperties storageProperties = Mockito.mock(StorageProperties.class);
        Mockito.when(storageProperties.getHadoopStorageConfig()).thenReturn(configuration);
        Mockito.when(storageProperties.getFsCacheFingerprint()).thenReturn("test-fingerprint");

        CatalogProperty catalogProperty = new CatalogProperty(null, Collections.emptyMap()) {
            @Override
            public Map<StorageProperties.Type, StorageProperties> getStoragePropertiesMap() {
                return Collections.singletonMap(StorageProperties.Type.HDFS, storageProperties);
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readerStarted = new CountDownLatch(1);
        try {
            Future<Map<String, String>> initializer = executor.submit(catalogProperty::getHadoopProperties);
            Assert.assertTrue(iterationStarted.await(5, TimeUnit.SECONDS));

            Future<Map<String, String>> concurrentReader = executor.submit(() -> {
                readerStarted.countDown();
                return new HashMap<>(catalogProperty.getHadoopProperties());
            });
            Assert.assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
            try {
                concurrentReader.get(200, TimeUnit.MILLISECONDS);
                Assert.fail("Concurrent readers must not observe a partially initialized cache");
            } catch (TimeoutException expected) {
                // The reader must wait for the initializing thread to publish the completed map.
            }

            allowIteration.countDown();
            Assert.assertEquals("complete", initializer.get(5, TimeUnit.SECONDS).get("fs.test.property"));
            Assert.assertEquals("complete", concurrentReader.get(5, TimeUnit.SECONDS).get("fs.test.property"));
        } finally {
            allowIteration.countDown();
            executor.shutdownNow();
        }
    }

    private static class BlockingConfiguration extends Configuration {
        private final CountDownLatch iterationStarted;
        private final CountDownLatch allowIteration;

        BlockingConfiguration(CountDownLatch iterationStarted, CountDownLatch allowIteration) {
            super(false);
            this.iterationStarted = iterationStarted;
            this.allowIteration = allowIteration;
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            iterationStarted.countDown();
            try {
                if (!allowIteration.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting to continue Hadoop configuration initialization");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while initializing Hadoop configuration", e);
            }
            return super.iterator();
        }
    }
}
