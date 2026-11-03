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

package org.apache.doris.catalog;

import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TAIResource;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AIResourceConcurrencyTest {
    private ImmutableMap<String, String> properties(String suffix) {
        return ImmutableMap.<String, String>builder()
                .put("ai.provider_type", "LOCAL")
                .put("ai.endpoint", "http://localhost:8000/" + suffix)
                .put("ai.model_name", "model-" + suffix)
                .put("ai.api_key", "key-" + suffix)
                .put("ai.embed.provider_type", "LOCAL")
                .put("ai.embed.endpoint", "http://localhost:8001/" + suffix)
                .put("ai.embed.model_name", "embed-" + suffix)
                .put("ai.embed.api_key", "embed-key-" + suffix)
                .put("ai.embed.mm.provider_type", "LOCAL")
                .put("ai.embed.mm.endpoint", "http://localhost:8002/" + suffix)
                .put("ai.embed.mm.model_name", "multimodal-" + suffix)
                .put("ai.embed.mm.api_key", "multimodal-key-" + suffix)
                .build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConcurrentAlterKeepsCompleteSnapshots(boolean restored) throws Exception {
        AIResource created = new AIResource("concurrent-snapshots");
        created.setProperties(properties("0"));
        AIResource resource = restored
                ? (AIResource) GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(created), Resource.class) : created;
        Map<String, String> original = resource.getCopiedProperties();
        TAIResource originalThrift = resource.toThrift();
        AIResource alternate = new AIResource("alternate-snapshot");
        alternate.setProperties(properties("1"));
        Map<String, String> changed = alternate.getCopiedProperties();
        TAIResource changedThrift = alternate.toThrift();
        ImmutableMap<String, String> zero = properties("0");
        ImmutableMap<String, String> one = properties("1");
        CountDownLatch ready = new CountDownLatch(3);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<?> writer = executor.submit(() -> {
                ready.countDown();
                Assertions.assertTrue(start.await(10, TimeUnit.SECONDS));
                for (int i = 1; i <= 1000; ++i) {
                    resource.modifyProperties(i % 2 == 0 ? zero : one);
                }
                return null;
            });
            Future<?> reader = executor.submit(() -> {
                ready.countDown();
                Assertions.assertTrue(start.await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 1000; ++i) {
                    Map<String, String> snapshot = resource.getCopiedProperties();
                    Assertions.assertTrue(snapshot.equals(original) || snapshot.equals(changed));
                    TAIResource thrift = resource.toThrift();
                    Assertions.assertTrue(thrift.equals(originalThrift) || thrift.equals(changedThrift));
                    Assertions.assertTrue(resource.hasCompleteGeneralProperties());
                    Assertions.assertTrue(resource.hasCompleteEmbedProperties());
                    Assertions.assertTrue(resource.hasCompleteMultimodalEmbedProperties());
                }
                return null;
            });
            Future<?> procReader = executor.submit(() -> {
                ready.countDown();
                Assertions.assertTrue(start.await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 1000; ++i) {
                    BaseProcResult result = new BaseProcResult();
                    resource.getProcNodeData(result);
                    long version = Long.parseLong(procValue(result.getRows(), "version"));
                    String suffix = Long.toString((version + 1) % 2);
                    Assertions.assertEquals("model-" + suffix, procValue(result.getRows(), "ai.model_name"));
                    Assertions.assertEquals("embed-" + suffix, procValue(result.getRows(), "ai.embed.model_name"));
                    Assertions.assertEquals("multimodal-" + suffix,
                            procValue(result.getRows(), "ai.embed.mm.model_name"));
                    Assertions.assertEquals("******", procValue(result.getRows(), "ai.api_key"));
                }
                return null;
            });
            Assertions.assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            writer.get(30, TimeUnit.SECONDS);
            reader.get(30, TimeUnit.SECONDS);
            procReader.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(999, resource.getVersion());
            Assertions.assertEquals(original, resource.getCopiedProperties());
            Assertions.assertEquals("model-0", resource.getProperty("ai.model_name"));
            original.put("ai.model_name", "external-change");
            Assertions.assertEquals("model-0", resource.getProperty("ai.model_name"));
            Assertions.assertEquals("model-0", originalThrift.getModelName());
        } finally {
            start.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private String procValue(List<List<String>> rows, String key) {
        return rows.stream().filter(row -> key.equals(row.get(2)))
                .map(row -> row.get(3)).findFirst().orElseThrow(AssertionError::new);
    }
}
