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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AnalyticWindow.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.thrift.TAnalyticWindow;
import org.apache.doris.thrift.TAnalyticWindowBoundary;
import org.apache.doris.thrift.TAnalyticWindowBoundaryType;
import org.apache.doris.thrift.TAnalyticWindowType;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Windowing clause of an analytic expr
 * Both left and right boundaries are always non-null after analyze().
 */
public class AnalyticWindow {
    // default window used when an analytic expr was given an order by but no window
    public static final AnalyticWindow DEFAULT_WINDOW = new AnalyticWindow(Type.RANGE,
            new Boundary(BoundaryType.UNBOUNDED_PRECEDING, null),
            new Boundary(BoundaryType.CURRENT_ROW, null));

    public enum Type {
        ROWS("ROWS"),
        RANGE("RANGE");

        private final String description;

        private Type(String d) {
            description = d;
        }

        @Override
        public String toString() {
            return description;
        }

        public TAnalyticWindowType toThrift() {
            return this == ROWS ? TAnalyticWindowType.ROWS : TAnalyticWindowType.RANGE;
        }
    }

    public enum BoundaryType {
        UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"),
        UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING"),
        CURRENT_ROW("CURRENT ROW"),
        PRECEDING("PRECEDING"),
        FOLLOWING("FOLLOWING");

        private final String description;

        private BoundaryType(String d) {
            description = d;
        }

        @Override
        public String toString() {
            return description;
        }

        public TAnalyticWindowBoundaryType toThrift() {
            Preconditions.checkState(!isAbsolutePos());

            if (this == CURRENT_ROW) {
                return TAnalyticWindowBoundaryType.CURRENT_ROW;
            } else if (this == PRECEDING) {
                return TAnalyticWindowBoundaryType.PRECEDING;
            } else if (this == FOLLOWING) {
                return TAnalyticWindowBoundaryType.FOLLOWING;
            }

            return null;
        }

        public boolean isAbsolutePos() {
            return this == UNBOUNDED_PRECEDING || this == UNBOUNDED_FOLLOWING;
        }

        public boolean isOffset() {
            return this == PRECEDING || this == FOLLOWING;
        }

        public boolean isPreceding() {
            return this == UNBOUNDED_PRECEDING || this == PRECEDING;
        }

        public boolean isFollowing() {
            return this == UNBOUNDED_FOLLOWING || this == FOLLOWING;
        }

        public BoundaryType converse() {
            switch (this) {
                case UNBOUNDED_PRECEDING:
                    return UNBOUNDED_FOLLOWING;

                case UNBOUNDED_FOLLOWING:
                    return UNBOUNDED_PRECEDING;

                case PRECEDING:
                    return FOLLOWING;

                case FOLLOWING:
                    return PRECEDING;

                default:
                    return CURRENT_ROW;
            }
        }
    }

    public static class Boundary {
        private final BoundaryType type;

        // Offset expr. Only set for PRECEDING/FOLLOWING. Needed for toSql().
        private final Expr expr;

        // The offset value. Set during analysis after evaluating expr_. Integral valued
        // for ROWS windows.
        private BigDecimal offsetValue;

        public BoundaryType getType() {
            return type;
        }

        public Expr getExpr() {
            return expr;
        }

        public Boundary(BoundaryType type, Expr e) {
            this(type, e, null);
        }

        // c'tor used by clone()
        public Boundary(BoundaryType type, Expr e, BigDecimal offsetValue) {
            Preconditions.checkState(
                    (type.isOffset() && e != null)
                    || (!type.isOffset() && e == null));
            this.type = type;
            this.expr = e;
            this.offsetValue = offsetValue;
        }

        public String toSql() {
            StringBuilder sb = new StringBuilder();

            if (expr != null) {
                sb.append(expr.toSql()).append(" ");
            }

            sb.append(type.toString());
            return sb.toString();
        }

        public String toSql(boolean disableTableName, boolean needExternalSql, TableType tableType,
                TableIf table) {
            StringBuilder sb = new StringBuilder();

            if (expr != null) {
                sb.append(expr.toSql(disableTableName, needExternalSql, tableType, table)).append(" ");
            }

            sb.append(type.toString());
            return sb.toString();
        }

        public String toDigest() {
            StringBuilder sb = new StringBuilder();

            if (expr != null) {
                sb.append(expr.toDigest()).append(" ");
            }

            sb.append(type.toString());
            return sb.toString();
        }

        public TAnalyticWindowBoundary toThrift(Type windowType) {
            TAnalyticWindowBoundary result = new TAnalyticWindowBoundary(type.toThrift());

            if (type.isOffset() && windowType == Type.ROWS) {
                result.setRowsOffsetValue(offsetValue.longValue());
            }

            // TODO: range windows need range_offset_predicate
            return result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, expr);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj.getClass() != this.getClass()) {
                return false;
            }

            Boundary o = (Boundary) obj;
            boolean exprEqual = (expr == null) == (o.expr == null);

            if (exprEqual && expr != null) {
                exprEqual = expr.equals(o.expr);
            }

            return type == o.type && exprEqual;
        }

        public Boundary converse() {
            Boundary result = new Boundary(type.converse(),
                    (expr != null) ? expr.clone() : null);
            result.offsetValue = offsetValue;
            return result;
        }

        @Override
        public Boundary clone() {
            return new Boundary(type, expr != null ? expr.clone() : null, offsetValue);
        }
    }

    private final Type type;
    private final Boundary leftBoundary;
    private Boundary rightBoundary;  // may be null before analyze()
    private String toSqlString;  // cached after analysis

    public Type getType() {
        return type;
    }

    public Boundary getLeftBoundary() {
        return leftBoundary;
    }

    public Boundary getRightBoundary() {
        return rightBoundary;
    }

    public Boundary setRightBoundary(Boundary b) {
        return rightBoundary = b;
    }

    public AnalyticWindow(Type type, Boundary b) {
        this.type = type;
        Preconditions.checkNotNull(b);
        leftBoundary = b;
        rightBoundary = null;
    }

    public AnalyticWindow(Type type, Boundary l, Boundary r) {
        this.type = type;
        Preconditions.checkNotNull(l);
        leftBoundary = l;
        Preconditions.checkNotNull(r);
        rightBoundary = r;
    }

    /**
     * Clone c'tor
     */
    private AnalyticWindow(AnalyticWindow other) {
        type = other.type;
        Preconditions.checkNotNull(other.leftBoundary);
        leftBoundary = other.leftBoundary.clone();

        if (other.rightBoundary != null) {
            rightBoundary = other.rightBoundary.clone();
        }

        toSqlString = other.toSqlString;  // safe to share
    }

    public AnalyticWindow reverse() {
        Boundary newRightBoundary = leftBoundary.converse();
        Boundary newLeftBoundary = null;

        if (rightBoundary == null) {
            newLeftBoundary = new Boundary(leftBoundary.getType(), null);
        } else {
            newLeftBoundary = rightBoundary.converse();
        }

        return new AnalyticWindow(type, newLeftBoundary, newRightBoundary);
    }

    public String toSql() {
        if (toSqlString != null) {
            return toSqlString;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(type.toString()).append(" ");

        if (rightBoundary == null) {
            sb.append(leftBoundary.toSql());
        } else {
            sb.append("BETWEEN ").append(leftBoundary.toSql()).append(" AND ");
            sb.append(rightBoundary.toSql());
        }

        return sb.toString();
    }

    public String toSql(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        if (toSqlString != null) {
            return toSqlString;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(type.toString()).append(" ");

        if (rightBoundary == null) {
            sb.append(leftBoundary.toSql(disableTableName, needExternalSql, tableType, table));
        } else {
            sb.append("BETWEEN ").append(leftBoundary.toSql(disableTableName, needExternalSql, tableType, table))
                    .append(" AND ");
            sb.append(rightBoundary.toSql(disableTableName, needExternalSql, tableType, table));
        }

        return sb.toString();
    }

    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append(type.toString()).append(" ");

        if (rightBoundary == null) {
            sb.append(leftBoundary.toDigest());
        } else {
            sb.append("BETWEEN ").append(leftBoundary.toDigest()).append(" AND ");
            sb.append(rightBoundary.toDigest());
        }

        return sb.toString();
    }


    public TAnalyticWindow toThrift() {
        TAnalyticWindow result = new TAnalyticWindow(type.toThrift());

        if (leftBoundary.getType() != BoundaryType.UNBOUNDED_PRECEDING) {
            result.setWindowStart(leftBoundary.toThrift(type));
        }

        Preconditions.checkNotNull(rightBoundary);

        if (rightBoundary.getType() != BoundaryType.UNBOUNDED_FOLLOWING) {
            result.setWindowEnd(rightBoundary.toThrift(type));
        }

        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, leftBoundary, rightBoundary);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        AnalyticWindow o = (AnalyticWindow) obj;
        boolean rightBoundaryEqual = (rightBoundary == null) == (o.rightBoundary == null);

        if (rightBoundaryEqual && rightBoundary != null) {
            rightBoundaryEqual = rightBoundary.equals(o.rightBoundary);
        }

        return type == o.type
               && leftBoundary.equals(o.leftBoundary)
               && rightBoundaryEqual;
    }

    @Override
    public AnalyticWindow clone() {
        return new AnalyticWindow(this);
    }
}
