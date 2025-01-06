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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.Optional;

/**
 * window frame
 */
public class WindowFrame extends Expression implements PropagateNullable, LeafExpression {

    private final FrameUnitsType frameUnits;

    private final FrameBoundary leftBoundary;

    private final FrameBoundary rightBoundary;

    public WindowFrame(FrameUnitsType frameUnits, FrameBoundary leftBoundary) {
        this(frameUnits, leftBoundary, new FrameBoundary(FrameBoundType.EMPTY_BOUNDARY));
    }

    public WindowFrame(FrameUnitsType frameUnits, FrameBoundary leftBoundary, FrameBoundary rightBoundary) {
        super(ImmutableList.of());
        this.frameUnits = frameUnits;
        this.leftBoundary = leftBoundary;
        this.rightBoundary = rightBoundary;
    }

    public FrameUnitsType getFrameUnits() {
        return frameUnits;
    }

    public FrameBoundary getLeftBoundary() {
        return leftBoundary;
    }

    public FrameBoundary getRightBoundary() {
        return rightBoundary;
    }

    /**
     * reverse left & right boundary; reverse each boundary's upper and lower bound
     */
    public WindowFrame reverseWindow() {
        return new WindowFrame(frameUnits, rightBoundary.reverse(), leftBoundary.reverse());
    }

    public WindowFrame withFrameUnits(FrameUnitsType newFrameUnits) {
        return new WindowFrame(newFrameUnits, leftBoundary, rightBoundary);
    }

    public WindowFrame withRightBoundary(FrameBoundary newRightBoundary) {
        return new WindowFrame(frameUnits, leftBoundary, newRightBoundary);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowFrame other = (WindowFrame) o;
        return Objects.equals(this.frameUnits, other.frameUnits)
            && Objects.equals(this.leftBoundary, other.leftBoundary)
            && Objects.equals(this.rightBoundary, other.rightBoundary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(frameUnits, leftBoundary, rightBoundary);
    }

    @Override
    public String computeToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(frameUnits + " ");
        if (rightBoundary != null) {
            sb.append("BETWEEN " + leftBoundary.toSql() + " AND " + rightBoundary.toSql());
        } else {
            sb.append(leftBoundary);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("WindowFrame(");
        sb.append(frameUnits + ", ");
        sb.append(leftBoundary + ", ");
        if (rightBoundary != null) {
            sb.append(rightBoundary);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWindowFrame(this, context);
    }

    /**
     * frame units types
     */
    public enum FrameUnitsType {
        ROWS,
        RANGE
    }

    /**
     * frame boundary
     */
    public static class FrameBoundary {

        private final Optional<Expression> boundOffset;
        private final FrameBoundType frameBoundType;

        public FrameBoundary(FrameBoundType frameBoundType) {
            this.boundOffset = Optional.empty();
            this.frameBoundType = frameBoundType;
        }

        public FrameBoundary(Optional<Expression> boundOffset, FrameBoundType frameBoundType) {
            this.boundOffset = boundOffset;
            this.frameBoundType = frameBoundType;
        }

        public static FrameBoundary newPrecedingBoundary() {
            return new FrameBoundary(FrameBoundType.UNBOUNDED_PRECEDING);
        }

        public static FrameBoundary newPrecedingBoundary(Expression boundValue) {
            return new FrameBoundary(Optional.of(boundValue), FrameBoundType.PRECEDING);
        }

        public static FrameBoundary newFollowingBoundary() {
            return new FrameBoundary(FrameBoundType.UNBOUNDED_FOLLOWING);
        }

        public static FrameBoundary newFollowingBoundary(Expression boundValue) {
            return new FrameBoundary(Optional.of(boundValue), FrameBoundType.FOLLOWING);
        }

        public static FrameBoundary newCurrentRowBoundary() {
            return new FrameBoundary(FrameBoundType.CURRENT_ROW);
        }

        public boolean is(FrameBoundType otherType) {
            return this.frameBoundType == otherType;
        }

        public boolean isNot(FrameBoundType otherType) {
            return this.frameBoundType != otherType;
        }

        public boolean isNull() {
            return this.frameBoundType == FrameBoundType.EMPTY_BOUNDARY;
        }

        public boolean hasOffset() {
            return frameBoundType == FrameBoundType.PRECEDING || frameBoundType == FrameBoundType.FOLLOWING;
        }

        public boolean asPreceding() {
            return frameBoundType == FrameBoundType.PRECEDING || frameBoundType == FrameBoundType.UNBOUNDED_PRECEDING;
        }

        public boolean asFollowing() {
            return frameBoundType == FrameBoundType.FOLLOWING || frameBoundType == FrameBoundType.UNBOUNDED_FOLLOWING;
        }

        public FrameBoundary reverse() {
            return new FrameBoundary(boundOffset, frameBoundType.reverse());
        }

        public FrameBoundType getFrameBoundType() {
            return frameBoundType;
        }

        public Optional<Expression> getBoundOffset() {
            return boundOffset;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boundOffset.ifPresent(value -> sb.append(value + " "));
            sb.append(frameBoundType);

            return sb.toString();
        }

        /** toSql*/
        public String toSql() {
            StringBuilder sb = new StringBuilder();
            boundOffset.ifPresent(value -> sb.append(value + " "));
            switch (frameBoundType) {
                case UNBOUNDED_PRECEDING:
                    sb.append("UNBOUNDED PRECEDING");
                    break;
                case UNBOUNDED_FOLLOWING:
                    sb.append("UNBOUNDED FOLLOWING");
                    break;
                case CURRENT_ROW:
                    sb.append("CURRENT ROW");
                    break;
                case PRECEDING:
                    sb.append("PRECEDING");
                    break;
                case FOLLOWING:
                    sb.append("FOLLOWING");
                    break;
                default:
                    break;
            }
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FrameBoundary other = (FrameBoundary) o;
            return Objects.equals(this.frameBoundType, other.frameBoundType)
                && Objects.equals(this.boundOffset, other.boundOffset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(boundOffset, frameBoundType);
        }
    }

    /**
     * frame bound types
     */
    public enum FrameBoundType {

        UNBOUNDED_PRECEDING,
        UNBOUNDED_FOLLOWING,
        CURRENT_ROW,
        PRECEDING,
        FOLLOWING,

        // represents that the boundary is null. We use this value as default
        // to avoid checking if a boundary is null frequently.
        EMPTY_BOUNDARY;

        /**
         * reverse current FrameBoundType
         */
        public FrameBoundType reverse() {
            switch (this) {
                case UNBOUNDED_PRECEDING:
                    return UNBOUNDED_FOLLOWING;
                case UNBOUNDED_FOLLOWING:
                    return UNBOUNDED_PRECEDING;
                case PRECEDING:
                    return FOLLOWING;
                case FOLLOWING:
                    return PRECEDING;
                case CURRENT_ROW:
                    return CURRENT_ROW;
                default:
                    return EMPTY_BOUNDARY;
            }
        }

        public boolean isFollowing() {
            return this.equals(UNBOUNDED_FOLLOWING) || this.equals(FOLLOWING);
        }

    }
}
