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

package org.apache.doris.connector.api.mvcc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable, source-agnostic description of an explicit time-travel request that
 * fe-core extracts from the SQL and hands to the connector to resolve into a
 * pinned {@link ConnectorMvccSnapshot}.
 *
 * <p>fe-core performs only source-agnostic dispatch/extraction (deciding the
 * {@link Kind}, splitting out the raw string / params, and the digital flag for
 * timestamps); the connector owns ALL provider-specific parsing (e.g. paimon
 * snapshot-id lookup, datetime parsing, tag/branch resolution, incremental
 * window validation).</p>
 *
 * <p>Each {@link Kind} maps to a piece of Doris time-travel syntax:</p>
 * <ul>
 *   <li>{@link Kind#SNAPSHOT_ID} &mdash; {@code FOR VERSION AS OF <id>}:
 *       {@code stringValue} holds the snapshot-id digits.</li>
 *   <li>{@link Kind#TIMESTAMP} &mdash; {@code FOR TIME AS OF <expr>}:
 *       {@code stringValue} holds either an epoch-millis literal (when
 *       {@code digital} is {@code true}) or a datetime string to be parsed by
 *       the connector (when {@code digital} is {@code false}).</li>
 *   <li>{@link Kind#TAG} &mdash; {@code @tag('name')} scan param:
 *       {@code stringValue} holds the tag name.</li>
 *   <li>{@link Kind#BRANCH} &mdash; {@code @branch('name')} scan param:
 *       {@code stringValue} holds the branch name.</li>
 *   <li>{@link Kind#INCREMENTAL} &mdash; {@code @incr(...)} scan param:
 *       {@code stringValue} is {@code null} and {@code incrementalParams}
 *       carries the raw key/value window arguments.</li>
 * </ul>
 */
public final class ConnectorTimeTravelSpec {

    /** Which flavor of explicit time-travel this spec describes. */
    public enum Kind {
        /** {@code FOR VERSION AS OF <id>}. */
        SNAPSHOT_ID,
        /** {@code FOR TIME AS OF <expr>}. */
        TIMESTAMP,
        /** {@code @tag('name')}. */
        TAG,
        /** {@code @branch('name')}. */
        BRANCH,
        /** {@code @incr(...)}. */
        INCREMENTAL
    }

    private final Kind kind;
    private final String stringValue;
    private final boolean digital;
    private final Map<String, String> incrementalParams;

    private ConnectorTimeTravelSpec(Kind kind, String stringValue, boolean digital,
            Map<String, String> incrementalParams) {
        this.kind = kind;
        this.stringValue = stringValue;
        this.digital = digital;
        this.incrementalParams = (incrementalParams == null || incrementalParams.isEmpty())
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(incrementalParams));
    }

    /**
     * {@code FOR VERSION AS OF <id>}: pin by snapshot id.
     *
     * @param idDigits the snapshot-id digits (connector parses to a number)
     */
    public static ConnectorTimeTravelSpec snapshotId(String idDigits) {
        Objects.requireNonNull(idDigits, "idDigits");
        return new ConnectorTimeTravelSpec(Kind.SNAPSHOT_ID, idDigits, false, null);
    }

    /**
     * {@code FOR TIME AS OF <expr>}: pin by wall-clock time.
     *
     * @param value   epoch-millis literal when {@code digital} is true, otherwise a
     *                datetime string the connector parses with the session time zone
     * @param digital whether {@code value} is already epoch-millis
     */
    public static ConnectorTimeTravelSpec timestamp(String value, boolean digital) {
        Objects.requireNonNull(value, "value");
        return new ConnectorTimeTravelSpec(Kind.TIMESTAMP, value, digital, null);
    }

    /**
     * {@code @tag('name')}: pin to a named tag.
     *
     * @param name the tag name
     */
    public static ConnectorTimeTravelSpec tag(String name) {
        Objects.requireNonNull(name, "name");
        return new ConnectorTimeTravelSpec(Kind.TAG, name, false, null);
    }

    /**
     * {@code @branch('name')}: pin to a named branch.
     *
     * @param name the branch name
     */
    public static ConnectorTimeTravelSpec branch(String name) {
        Objects.requireNonNull(name, "name");
        return new ConnectorTimeTravelSpec(Kind.BRANCH, name, false, null);
    }

    /**
     * {@code @incr(...)}: incremental read over a window described by
     * {@code rawParams}. The connector validates and interprets the window keys.
     *
     * @param rawParams the raw key/value window arguments (defensively copied)
     */
    public static ConnectorTimeTravelSpec incremental(Map<String, String> rawParams) {
        Objects.requireNonNull(rawParams, "rawParams");
        return new ConnectorTimeTravelSpec(Kind.INCREMENTAL, null, false, rawParams);
    }

    /** The flavor of this spec; never null. */
    public Kind getKind() {
        return kind;
    }

    /**
     * The snapshot-id digits / timestamp expression / tag name / branch name,
     * depending on {@link #getKind()}. {@code null} for {@link Kind#INCREMENTAL}.
     */
    public String getStringValue() {
        return stringValue;
    }

    /**
     * Only meaningful for {@link Kind#TIMESTAMP}: {@code true} means
     * {@link #getStringValue()} is already epoch-millis, {@code false} means it is
     * a datetime string the connector must parse. Always {@code false} otherwise.
     */
    public boolean isDigital() {
        return digital;
    }

    /**
     * The raw incremental window arguments; non-empty only for
     * {@link Kind#INCREMENTAL}, an unmodifiable empty map otherwise. Never null.
     */
    public Map<String, String> getIncrementalParams() {
        return incrementalParams;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorTimeTravelSpec)) {
            return false;
        }
        ConnectorTimeTravelSpec that = (ConnectorTimeTravelSpec) o;
        return digital == that.digital
                && kind == that.kind
                && Objects.equals(stringValue, that.stringValue)
                && incrementalParams.equals(that.incrementalParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, stringValue, digital, incrementalParams);
    }

    @Override
    public String toString() {
        return "ConnectorTimeTravelSpec{"
                + "kind=" + kind
                + ", stringValue=" + stringValue
                + ", digital=" + digital
                + ", incrementalParams=" + incrementalParams
                + '}';
    }
}
