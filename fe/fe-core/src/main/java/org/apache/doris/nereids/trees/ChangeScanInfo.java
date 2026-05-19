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

package org.apache.doris.nereids.trees;

import java.util.Objects;
import java.util.Optional;

/**
 * Options for single-table incremental binlog query on base table.
 *
 * This is a pure metadata holder used by Nereids planner and FE planner to
 * carry information from parsed table@incr(...) down to scan node.
 */
public class ChangeScanInfo {

    /**
     * What kind of change information the query expects.
     * DEFAULT in SQL is mapped to MIN_DELTA here.
     */
    public enum InformationKind {
        MIN_DELTA,
        APPEND_ONLY,
        DETAIL
    }

    /**
     * The way user specifies the incremental position.
     */
    public enum PositionKind {
        VERSION,
        TIMESTAMP,
        OFFSET
    }

    /**
     * A single position in incremental query (used for both start and end).
     *
     */
    public static class Position {
        public final PositionKind kind;
        public final long value;

        public Position(PositionKind kind, long value) {
            this.kind = Objects.requireNonNull(kind, "kind should not be null");
            this.value = value;
        }

        public static Position forTimestamp(long timestampLiteral) {
            return new Position(PositionKind.TIMESTAMP, timestampLiteral);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Position)) {
                return false;
            }
            Position that = (Position) o;
            return value == that.value && kind == that.kind;
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, value);
        }
    }

    private final InformationKind informationKind;
    private final Position at;
    private final Optional<Position> end;

    public ChangeScanInfo(InformationKind informationKind, Position at, Optional<Position> end) {
        this.informationKind = Objects.requireNonNull(informationKind, "informationKind should not be null");
        this.at = Objects.requireNonNull(at, "at should not be null");
        this.end = Objects.requireNonNull(end, "end should not be null");
    }

    public InformationKind getInformationKind() {
        return informationKind;
    }

    public Position getAt() {
        return at;
    }

    public Optional<Position> getEnd() {
        return end;
    }

    /**
     * Generate digest string used in normalized sql cache.
     */
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("@incr(\"incrementType\" = \"");
        if (informationKind == InformationKind.MIN_DELTA) {
            sb.append("DEFAULT");
        } else if (informationKind == InformationKind.DETAIL) {
            sb.append("DETAIL");
        } else {
            sb.append("APPEND_ONLY");
        }
        sb.append("\", \"startTimestamp\" = ");
        sb.append(positionToDigest(at));
        if (end.isPresent()) {
            sb.append(", \"endTimestamp\" = ");
            sb.append(positionToDigest(end.get()));
        }
        sb.append(")");
        return sb.toString();
    }

    private String positionToDigest(Position pos) {
        switch (pos.kind) {
            case TIMESTAMP:
                return "?";
            default:
                return "?";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChangeScanInfo)) {
            return false;
        }
        ChangeScanInfo that = (ChangeScanInfo) o;
        return informationKind == that.informationKind && at.equals(that.at) && end.equals(that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(informationKind, at, end);
    }
}
