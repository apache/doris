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

import org.apache.doris.common.util.TimeUtils;

/**
 * Represents a timed phase within a query's execution. Created by {@link QueryTrace#startSpan(String)},
 * must be used with try-with-resources to ensure proper timing and Span stack management.
 *
 * <p>On {@link #close()}, the span automatically:
 * <ol>
 *   <li>Computes elapsed time and accumulates it into the corresponding {@link QueryTrace} entry</li>
 *   <li>Pops itself from the ThreadLocal Span stack, restoring the parent Span as the current active Span</li>
 * </ol>
 *
 * <p>Usage:
 * <pre>{@code
 * try (ProfileSpan span = trace.startSpan("Nereids Analysis Time")) {
 *     // timed logic
 * }
 * }</pre>
 *
 * <p>If the same span name is started and closed multiple times, durations are accumulated.
 */
public class ProfileSpan implements AutoCloseable {
    /**
     * A no-op span that does nothing on close(). Used when no QueryTrace is available.
     */
    public static final ProfileSpan NO_OP = new ProfileSpan("no-op", null, null) {
        @Override
        public void close() {
            // no-op
        }
    };

    private final String name;
    private final QueryTrace trace;
    private final ProfileSpan parent;
    private final long startTimeMs;

    ProfileSpan(String name, QueryTrace trace, ProfileSpan parent) {
        this.name = name;
        this.trace = trace;
        this.parent = parent;
        this.startTimeMs = TimeUtils.getStartTimeMs();
    }

    @Override
    public void close() {
        long durationMs = TimeUtils.getStartTimeMs() - startTimeMs;
        trace.recordSpanDuration(name, durationMs);
        trace.popCurrentSpan(this);
    }

    public String getName() {
        return name;
    }

    public ProfileSpan getParent() {
        return parent;
    }

    /**
     * Get the elapsed time since this span was started (for in-flight sampling).
     */
    public long getElapsedMs() {
        return TimeUtils.getStartTimeMs() - startTimeMs;
    }
}
