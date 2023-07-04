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

package org.apache.doris.common;

import org.apache.doris.common.util.CacheBulkLoader;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections.MapUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CacheBulkLoaderTest {

    @Test
    public void test() {
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                10,
                1000,
                "TestThreadPool", 120, true);

        LoadingCache<String, String> testCache = CacheBuilder.newBuilder().maximumSize(100)
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .build(new CacheBulkLoader<String, String>() {
                    @Override
                    protected ExecutorService getExecutor() {
                        return executor;
                    }

                    @Override
                    public String load(String key) {
                        Assertions.assertTrue(
                                Thread.currentThread().getName().startsWith("TestThreadPool")
                        );
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException interruptedException) {
                            interruptedException.printStackTrace();
                        }
                        return key.replace("k", "v");
                    }
                });

        List<String> testKeys = Arrays.asList("k1", "k2", "k3", "k4");
        try {
            Map<String, String> vMap = testCache.getAll(testKeys);
            Assertions.assertTrue(MapUtils.isNotEmpty(vMap) && vMap.size() == 4);
            for (String key : vMap.keySet()) {
                Assertions.assertTrue(key.replace("k", "v").equals(vMap.get(key)));
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        try {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
