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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ProfileTracer manages a collection of ProfileSpans and their registration
 * into a RuntimeProfile's counter hierarchy.
 *
 * Spans are created on-demand at call sites via factory methods. Parent-child
 * relationships are specified by parent span name strings. The tracer maintains
 * a name→span mapping to support cross-method span lookup.
 */
public class ProfileTracer {
    private final RuntimeProfile profile;
    private final ConcurrentMap<String, ProfileSpan> spans = new ConcurrentHashMap<>();

    public ProfileTracer(RuntimeProfile profile) {
        this.profile = profile;
    }

    /**
     * Create a top-level span (parent is ROOT_COUNTER), auto-started.
     */
    public ProfileSpan startSpan(String name, TUnit unit) {
        return startSpan(name, unit, RuntimeProfile.ROOT_COUNTER, false);
    }

    /**
     * Create a child span (parent looked up by name), auto-started.
     */
    public ProfileSpan startSpan(String name, TUnit unit, String parentName) {
        return startSpan(name, unit, parentName, false);
    }

    /**
     * Create an accumulative span (not auto-started, use addElapsed()).
     */
    public ProfileSpan createAccSpan(String name, TUnit unit, String parentName) {
        return createSpan(name, unit, parentName, true);
    }

    private ProfileSpan startSpan(String name, TUnit unit, String parentName, boolean accumulative) {
        ProfileSpan span = createSpan(name, unit, parentName, accumulative);
        if (!accumulative) {
            span.start();
        }
        return span;
    }

    private ProfileSpan createSpan(String name, TUnit unit, String parentName, boolean accumulative) {
        return spans.computeIfAbsent(name, n -> {
            // Ensure the parent counter exists in the profile before creating the child.
            // If the parent span hasn't been created yet (e.g. INSERT path where child
            // spans are created before their parent), auto-create the parent counter
            // under ROOT_COUNTER to preserve the intended hierarchy.
            if (!parentName.equals(RuntimeProfile.ROOT_COUNTER)
                    && !profile.getCounterMap().containsKey(parentName)) {
                profile.addCounter(parentName, unit, RuntimeProfile.ROOT_COUNTER);
            }
            Counter counter = profile.addCounter(n, unit, parentName);
            return new ProfileSpan(n, counter, accumulative);
        });
    }

    /** Get an existing span by name, or null if not found. */
    public ProfileSpan getSpan(String name) {
        return spans.get(name);
    }

    /** Convenience: finish a span by name. */
    public void finish(String name) {
        ProfileSpan span = spans.get(name);
        if (span != null) {
            span.finish();
        }
    }

    public RuntimeProfile getProfile() {
        return profile;
    }
}
