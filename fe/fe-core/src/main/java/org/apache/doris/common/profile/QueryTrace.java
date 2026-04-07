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

import com.google.common.base.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A per-query tracing container that dynamically collects timing spans, counters and text attributes
 * during FE-side query execution. Replaces the hardcoded field-based approach in SummaryProfile.
 *
 * <h3>Core Concepts:</h3>
 * <ul>
 *   <li><b>Span</b> – A timed phase, created via {@link #startSpan(String)} with try-with-resources.
 *       Multiple starts of the same name accumulate durations.</li>
 *   <li><b>Counter</b> – A numeric metric (count, bytes, rate), set or accumulated.</li>
 *   <li><b>Text</b> – A string attribute (version, workload group name, etc.).</li>
 * </ul>
 *
 * <h3>ThreadLocal Scope:</h3>
 * <p>A ThreadLocal Span stack automatically tracks the current active span.
 * When {@link #startSpan(String)} is called, the stack-top span is used as the parent.
 * This enables cross-class span nesting without parameter passing.</p>
 *
 * <h3>Display Order and Indentation:</h3>
 * <ul>
 *   <li>Display order = insertion order (first-seen order, maintained by LinkedHashMap)</li>
 *   <li>Indentation = parent chain depth (computed at registration time)</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * // Timed span
 * try (ProfileSpan span = trace.startSpan("Plan Time")) {
 *     try (ProfileSpan child = trace.startSpan("Nereids Analysis Time")) {
 *         // automatically a child of "Plan Time"
 *     }
 * }
 *
 * // Counter
 * trace.setCounter("Fragment Compressed Size", size, TUnit.BYTES);
 * trace.addCounter("Fragment RPC Count", 1);
 *
 * // Text
 * trace.setText("Doris Version", version);
 * }</pre>
 */
public class QueryTrace {

    /**
     * Type of a registered metric entry.
     */
    enum MetricType {
        SPAN,       // Timed span (AutoCloseable, durations accumulate)
        COUNTER,    // Numeric counter / gauge
        TEXT        // String attribute
    }

    /**
     * Internal data holder for a single registered metric.
     */
    static class MetricEntry {
        final String name;
        final String parentName;   // null for root-level entries
        final MetricType type;
        final TUnit unit;
        final int indentLevel;     // computed from parent chain depth

        // Numeric value storage (for SPAN durations and COUNTER values).
        // Use volatile for thread safety on simple set/get.
        // For accumulated spans/counters, synchronized(this) on the entry provides atomicity.
        volatile long numericValue;
        // Text value storage
        volatile String textValue;

        MetricEntry(String name, String parentName, MetricType type, TUnit unit, int indentLevel) {
            this.name = name;
            this.parentName = parentName;
            this.type = type;
            this.unit = unit;
            this.indentLevel = indentLevel;
            this.numericValue = 0;
            this.textValue = null;
        }

        String format() {
            switch (type) {
                case SPAN:
                case COUNTER:
                    return RuntimeProfile.printCounter(numericValue, unit);
                case TEXT:
                    return textValue != null ? textValue : "N/A";
                default:
                    return "N/A";
            }
        }
    }

    // ---- All metrics, keyed by name, insertion-ordered for display ----
    private final LinkedHashMap<String, MetricEntry> metrics = new LinkedHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // ---- ThreadLocal Span stack for automatic parent detection ----
    private final ThreadLocal<Deque<ProfileSpan>> spanStack = ThreadLocal.withInitial(ArrayDeque::new);

    // ============================================================
    // Span API
    // ============================================================

    /**
     * Start a new timed span. The parent is automatically detected from the ThreadLocal span stack.
     * Must be used with try-with-resources.
     *
     * <p>If the same span name is started multiple times, durations accumulate.
     *
     * @param name the display name of this span (e.g. "Nereids Analysis Time")
     * @return a ProfileSpan that must be closed via try-with-resources
     */
    public ProfileSpan startSpan(String name) {
        ProfileSpan parentSpan = peekCurrentSpan();
        String parentName = parentSpan != null ? parentSpan.getName() : null;

        ensureRegistered(name, parentName, MetricType.SPAN, TUnit.TIME_MS);

        ProfileSpan span = new ProfileSpan(name, this, parentSpan);
        pushCurrentSpan(span);
        return span;
    }

    /**
     * Record a pre-computed duration directly (for cases where try-with-resources is not feasible,
     * e.g. GC time computed as a delta between two JMX readings).
     *
     * <p>The parent is inferred from the current span stack. Multiple calls accumulate.
     *
     * @param name the display name (e.g. "Garbage Collect During Plan Time")
     * @param durationMs the duration in milliseconds
     */
    public void recordDuration(String name, long durationMs) {
        String parentName = getCurrentSpanName();
        ensureRegistered(name, parentName, MetricType.SPAN, TUnit.TIME_MS);
        MetricEntry entry = getEntry(name);
        if (entry != null) {
            synchronized (entry) {
                entry.numericValue += durationMs;
            }
        }
    }

    // ============================================================
    // Counter API
    // ============================================================

    /**
     * Set a counter value (overwrite semantics). Auto-registers on first use.
     * Parent is inferred from the current span stack.
     *
     * @param name the display name (e.g. "Fragment Compressed Size")
     * @param value the value
     * @param unit the display unit (e.g. TUnit.BYTES)
     */
    public void setCounter(String name, long value, TUnit unit) {
        String parentName = getCurrentSpanName();
        ensureRegistered(name, parentName, MetricType.COUNTER, unit);
        MetricEntry entry = getEntry(name);
        Preconditions.checkNotNull(entry);
        entry.numericValue = value;
    }

    /**
     * Add to a counter (accumulate semantics). Default unit is TUnit.UNIT.
     */
    public void addCounter(String name, long delta) {
        addCounter(name, delta, TUnit.UNIT);
    }

    /**
     * Add to a counter with a specified unit.
     */
    public void addCounter(String name, long delta, TUnit unit) {
        String parentName = getCurrentSpanName();
        ensureRegistered(name, parentName, MetricType.COUNTER, unit);
        MetricEntry entry = getEntry(name);
        Preconditions.checkNotNull(entry);
        synchronized (entry) {
            entry.numericValue += delta;
        }
    }

    // ============================================================
    // Text API
    // ============================================================

    /**
     * Set a text attribute. Auto-registers on first use.
     *
     * @param name the display name (e.g. "Doris Version")
     * @param value the text value
     */
    public void setText(String name, String value) {
        String parentName = getCurrentSpanName();
        ensureRegistered(name, parentName, MetricType.TEXT, TUnit.NONE);
        MetricEntry entry = getEntry(name);
        Preconditions.checkNotNull(entry);
        entry.textValue = value;
    }

    // ============================================================
    // Data Read API (for MetricRepo reporting, etc.)
    // ============================================================

    /**
     * Get the duration in milliseconds for a span. Returns -1 if not found.
     */
    public long getDurationMs(String name) {
        MetricEntry entry = getEntry(name);
        return entry != null ? entry.numericValue : -1;
    }

    /**
     * Get the counter value. Returns 0 if not found.
     */
    public long getCounterValue(String name) {
        MetricEntry entry = getEntry(name);
        return entry != null ? entry.numericValue : 0;
    }

    /**
     * Get the text value. Returns null if not found.
     */
    public String getTextValue(String name) {
        MetricEntry entry = getEntry(name);
        return entry != null ? entry.textValue : null;
    }

    // ============================================================
    // Output: populate RuntimeProfile
    // ============================================================

    /**
     * Populate a RuntimeProfile with all registered metrics, in insertion order.
     * Indentation is automatically applied based on parent-child depth.
     *
     * @param profile the RuntimeProfile to populate (typically the "Execution Summary" profile)
     */
    public void populateProfile(RuntimeProfile profile) {
        lock.readLock().lock();
        try {
            for (MetricEntry entry : metrics.values()) {
                String formatted = entry.format();
                if (formatted != null) {
                    profile.addInfoString(entry.name, formatted, entry.indentLevel);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    // ============================================================
    // Internal: Span stack management
    // ============================================================

    void pushCurrentSpan(ProfileSpan span) {
        spanStack.get().push(span);
    }

    void popCurrentSpan(ProfileSpan span) {
        Deque<ProfileSpan> stack = spanStack.get();
        if (!stack.isEmpty() && stack.peek() == span) {
            stack.pop();
        }
    }

    private ProfileSpan peekCurrentSpan() {
        Deque<ProfileSpan> stack = spanStack.get();
        return stack.isEmpty() ? null : stack.peek();
    }

    private String getCurrentSpanName() {
        ProfileSpan current = peekCurrentSpan();
        return current != null ? current.getName() : null;
    }

    // ============================================================
    // Internal: Span duration recording (called by ProfileSpan.close())
    // ============================================================

    void recordSpanDuration(String name, long durationMs) {
        MetricEntry entry = getEntry(name);
        if (entry != null) {
            synchronized (entry) {
                entry.numericValue += durationMs;
            }
        }
    }

    // ============================================================
    // Internal: Dynamic registration
    // ============================================================

    private void ensureRegistered(String name, String parentName, MetricType type, TUnit unit) {
        // Fast path: check with read lock
        lock.readLock().lock();
        try {
            if (metrics.containsKey(name)) {
                return;
            }
        } finally {
            lock.readLock().unlock();
        }
        // Slow path: register with write lock
        lock.writeLock().lock();
        try {
            if (!metrics.containsKey(name)) {
                int indent = 0;
                if (parentName != null) {
                    MetricEntry parentEntry = metrics.get(parentName);
                    if (parentEntry != null) {
                        indent = parentEntry.indentLevel + 1;
                    }
                }
                metrics.put(name, new MetricEntry(name, parentName, type, unit, indent));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private MetricEntry getEntry(String name) {
        lock.readLock().lock();
        try {
            return metrics.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }
}
