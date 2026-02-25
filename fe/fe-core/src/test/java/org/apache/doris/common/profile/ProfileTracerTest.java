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

package org.apache.doris.common.profile;

import org.apache.doris.thrift.TUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class ProfileTracerTest {

    @Test
    public void testStartTopLevelSpan() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        ProfileSpan span = tracer.startSpan("Parse SQL Time", TUnit.TIME_MS);
        Assertions.assertNotNull(span);
        Assertions.assertEquals("Parse SQL Time", span.getName());

        // Counter should be registered in the profile
        Counter counter = profile.getCounterMap().get("Parse SQL Time");
        Assertions.assertNotNull(counter);
    }

    @Test
    public void testStartChildSpan() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        // Create parent first
        tracer.startSpan("Plan Time", TUnit.TIME_MS);

        // Create child
        ProfileSpan child = tracer.startSpan("Analysis Time", TUnit.TIME_MS, "Plan Time");
        Assertions.assertNotNull(child);

        // Verify parent-child relationship in counter map
        Map<String, Set<String>> childCounterMap = profile.getChildCounterMap();
        Set<String> planChildren = childCounterMap.get("Plan Time");
        Assertions.assertNotNull(planChildren);
        Assertions.assertTrue(planChildren.contains("Analysis Time"));
    }

    @Test
    public void testGetSpan() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        Assertions.assertNull(tracer.getSpan("nonexistent"));

        ProfileSpan span = tracer.startSpan("Test Span", TUnit.TIME_MS);
        Assertions.assertSame(span, tracer.getSpan("Test Span"));
    }

    @Test
    public void testFinishByName() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        // Use accumulative span so setValue is not overwritten by finish()
        ProfileSpan span = tracer.createAccSpan("Test Span", TUnit.TIME_MS, "");
        span.setValue(42);

        tracer.finish("Test Span");
        // finish on a span that was never start()'d is a no-op
        Assertions.assertEquals(42, span.getValue());
    }

    @Test
    public void testFinishNonexistentSpanIsSafe() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        // Should not throw
        tracer.finish("nonexistent");
    }

    @Test
    public void testCreateAccSpan() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        tracer.startSpan("Parent", TUnit.TIME_MS);
        ProfileSpan accSpan = tracer.createAccSpan("GC Time", TUnit.TIME_MS, "Parent");

        // Accumulative span should not be auto-started
        accSpan.addElapsed(10);
        accSpan.addElapsed(20);
        Assertions.assertEquals(30, accSpan.getValue());
    }

    @Test
    public void testIdempotentSpanCreation() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        ProfileSpan span1 = tracer.startSpan("Test", TUnit.TIME_MS);
        ProfileSpan span2 = tracer.startSpan("Test", TUnit.TIME_MS);

        // Should return the same span instance
        Assertions.assertSame(span1, span2);
    }

    @Test
    public void testCounterHierarchyPreservesInsertionOrder() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);

        // Create parent
        tracer.startSpan("Plan Time", TUnit.TIME_MS);

        // Create children in specific order
        tracer.startSpan("Lock Table", TUnit.TIME_MS, "Plan Time");
        tracer.startSpan("Analysis", TUnit.TIME_MS, "Plan Time");
        tracer.startSpan("Rewrite", TUnit.TIME_MS, "Plan Time");
        tracer.startSpan("Optimize", TUnit.TIME_MS, "Plan Time");

        // Verify insertion order is preserved (LinkedHashSet)
        Set<String> children = profile.getChildCounterMap().get("Plan Time");
        Assertions.assertNotNull(children);
        String[] expected = {"Lock Table", "Analysis", "Rewrite", "Optimize"};
        String[] actual = children.toArray(new String[0]);
        Assertions.assertArrayEquals(expected, actual);
    }

    @Test
    public void testGetProfile() {
        RuntimeProfile profile = new RuntimeProfile("TestProfile");
        ProfileTracer tracer = new ProfileTracer(profile);
        Assertions.assertSame(profile, tracer.getProfile());
    }
}
