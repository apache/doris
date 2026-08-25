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

package org.apache.doris.qe;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryFinishCallbackRegistryTest {

    // A query-finish callback must actually run so connector cleanup
    // (e.g. committing a hive read transaction) happens at query end.
    @Test
    public void testRunAndClearRunsRegisteredCallback() {
        QueryFinishCallbackRegistry registry = new QueryFinishCallbackRegistry();
        AtomicInteger runs = new AtomicInteger(0);
        registry.register("q1", runs::incrementAndGet);

        registry.runAndClear("q1");

        Assert.assertEquals(1, runs.get());
    }

    // The generic query-cleanup path drains this registry for every query,
    // most of which register nothing; draining an unknown query must be a
    // harmless no-op (never throw).
    @Test
    public void testRunAndClearWithNoCallbackIsNoOp() {
        QueryFinishCallbackRegistry registry = new QueryFinishCallbackRegistry();
        registry.runAndClear("unknown-query");
    }

    // A single query may open several transactional scans; all their cleanup
    // callbacks must run, in the order they were registered.
    @Test
    public void testMultipleCallbacksRunInRegistrationOrder() {
        QueryFinishCallbackRegistry registry = new QueryFinishCallbackRegistry();
        List<Integer> order = Lists.newArrayList();
        registry.register("q1", () -> order.add(1));
        registry.register("q1", () -> order.add(2));
        registry.register("q1", () -> order.add(3));

        registry.runAndClear("q1");

        Assert.assertEquals(Lists.newArrayList(1, 2, 3), order);
    }

    // The early lock-release path can drain a query before its final drain at
    // unregisterQuery. The second drain must be a no-op so cleanup runs once.
    @Test
    public void testRunAndClearIsIdempotent() {
        QueryFinishCallbackRegistry registry = new QueryFinishCallbackRegistry();
        AtomicInteger runs = new AtomicInteger(0);
        registry.register("q1", runs::incrementAndGet);

        registry.runAndClear("q1");
        registry.runAndClear("q1");

        Assert.assertEquals(1, runs.get());
    }

    // One connector's failing cleanup must not block another's, nor break the
    // rest of query teardown: exceptions are isolated and runAndClear returns.
    @Test
    public void testFailingCallbackIsIsolated() {
        QueryFinishCallbackRegistry registry = new QueryFinishCallbackRegistry();
        AtomicInteger runs = new AtomicInteger(0);
        registry.register("q1", () -> {
            throw new RuntimeException("boom");
        });
        registry.register("q1", runs::incrementAndGet);

        registry.runAndClear("q1");

        Assert.assertEquals(1, runs.get());
    }

    // Cleanup is scoped to the finishing query: draining one query must not run
    // or discard callbacks registered for a different query.
    @Test
    public void testCallbacksAreScopedPerQuery() {
        QueryFinishCallbackRegistry registry = new QueryFinishCallbackRegistry();
        AtomicInteger q1Runs = new AtomicInteger(0);
        AtomicInteger q2Runs = new AtomicInteger(0);
        registry.register("q1", q1Runs::incrementAndGet);
        registry.register("q2", q2Runs::incrementAndGet);

        registry.runAndClear("q1");

        Assert.assertEquals(1, q1Runs.get());
        Assert.assertEquals(0, q2Runs.get());

        registry.runAndClear("q2");
        Assert.assertEquals(1, q2Runs.get());
    }
}
