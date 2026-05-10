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

package org.apache.doris.connector.api.pushdown;

import org.apache.doris.connector.api.ConnectorType;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A domain for a single column: a union of {@link ConnectorRange}s with
 * optional NULL inclusion. Used for fast partition pruning.
 *
 * <p>Example: {@code col IN (1,2,3)} → 3 single-value ranges, nullsAllowed=false.</p>
 */
public final class ConnectorDomain implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConnectorType type;
    private final List<ConnectorRange> ranges;
    private final boolean nullsAllowed;

    public ConnectorDomain(ConnectorType type,
            List<ConnectorRange> ranges, boolean nullsAllowed) {
        this.type = Objects.requireNonNull(type, "type");
        this.ranges = ranges == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(ranges);
        this.nullsAllowed = nullsAllowed;
    }

    /** Creates an all-pass domain (matches everything including null). */
    public static ConnectorDomain all(ConnectorType type) {
        return new ConnectorDomain(type,
                Collections.singletonList(ConnectorRange.all()), true);
    }

    /** Creates a none domain (matches nothing). */
    public static ConnectorDomain none(ConnectorType type) {
        return new ConnectorDomain(type, Collections.emptyList(), false);
    }

    /** Creates a single-value domain. */
    public static ConnectorDomain singleValue(ConnectorType type, Comparable<?> value) {
        return new ConnectorDomain(type,
                Collections.singletonList(ConnectorRange.equal(value)), false);
    }

    /** Creates a null-only domain. */
    public static ConnectorDomain onlyNull(ConnectorType type) {
        return new ConnectorDomain(type, Collections.emptyList(), true);
    }

    public ConnectorType getType() {
        return type;
    }

    public List<ConnectorRange> getRanges() {
        return ranges;
    }

    public boolean isNullsAllowed() {
        return nullsAllowed;
    }

    public boolean isAll() {
        return nullsAllowed && ranges.size() == 1 && ranges.get(0).isAll();
    }

    public boolean isNone() {
        return !nullsAllowed && ranges.isEmpty();
    }

    @Override
    public String toString() {
        return "ConnectorDomain{type=" + type
                + ", ranges=" + ranges
                + ", nullsAllowed=" + nullsAllowed + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorDomain)) {
            return false;
        }
        ConnectorDomain that = (ConnectorDomain) o;
        return nullsAllowed == that.nullsAllowed
                && type.equals(that.type) && ranges.equals(that.ranges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, ranges, nullsAllowed);
    }
}
