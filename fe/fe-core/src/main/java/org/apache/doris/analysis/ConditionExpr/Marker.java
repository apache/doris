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
package org.apache.doris.analysis.ConditionExpr;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.analysis.ConditionExpr.Utils.isCompatibleType;

/**
 * A point on the continuous space defined by the specified type.
 * Each point may be just below, exact, or just above the specified value according to the Bound.
 */
public final class Marker
        implements Comparable<Marker>
{
    public enum Bound
    {
        BELOW,   // lower than the value, but infinitesimally close to the value
        EXACTLY, // exactly the value
        ABOVE    // higher than the value, but infinitesimally close to the value
    }

    private final Type type;
    private final Optional<Object> valueBlock;
    private final Bound bound;

    /**
     * LOWER UNBOUNDED is specified with an empty value and a ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    public Marker(Type type, Optional<Object> valueBlock, Bound bound)
    {
        requireNonNull(type, "type is null");
        requireNonNull(valueBlock, "valueBlock is null");
        requireNonNull(bound, "bound is null");

        if (valueBlock.isEmpty() && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Cannot be equal to unbounded");
        }
        this.type = type;
        this.valueBlock = valueBlock;
        this.bound = bound;
    }

    private static Marker create(Type type, Optional<Object> value, Bound bound)
    {
        return new Marker(type, value, bound);
    }

    public static Marker upperUnbounded(Type type)
    {
        requireNonNull(type, "type is null");
        return create(type, Optional.empty(), Bound.BELOW);
    }

    public static Marker lowerUnbounded(Type type)
    {
        requireNonNull(type, "type is null");
        return create(type, Optional.empty(), Bound.ABOVE);
    }

    public static Marker above(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.ABOVE);
    }

    public static Marker exactly(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.EXACTLY);
    }

    public static Marker below(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(type, Optional.of(value), Bound.BELOW);
    }

    public Type getType()
    {
        return type;
    }

    public Object getValueBlock()
    {
        return valueBlock;
    }

    public Object getValue()
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No value to get");
        }
        return valueBlock.get();
    }

    public Bound getBound()
    {
        return bound;
    }

    public boolean isUpperUnbounded()
    {
        return valueBlock.isEmpty() && bound == Bound.BELOW;
    }

    public boolean isLowerUnbounded()
    {
        return valueBlock.isEmpty() && bound == Bound.ABOVE;
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!isCompatibleType(getType(), marker.getType())) {
            throw new IllegalArgumentException("Mismatched Marker types: "+type);
        }
    }

    /**
     * Adjacency is defined by two Markers being infinitesimally close to each other.
     * This means they must share the same value and have adjacent Bounds.
     */
    public boolean isAdjacent(Marker other)
    {
        checkTypeCompatibility(other);
        if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
            return false;
        }
        if (compare(valueBlock.get(), type, other.valueBlock.get(), other.type) != 0) {
            return false;
        }
        return (bound == Bound.EXACTLY && other.bound != Bound.EXACTLY) ||
                (bound != Bound.EXACTLY && other.bound == Bound.EXACTLY);
    }

    public Marker greaterAdjacent()
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new Marker(type, valueBlock, Bound.EXACTLY);
            case EXACTLY:
                return new Marker(type, valueBlock, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public Marker lesserAdjacent()
    {
        if (valueBlock.isEmpty()) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker(type, valueBlock, Bound.BELOW);
            case ABOVE:
                return new Marker(type, valueBlock, Bound.EXACTLY);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    @Override
    public int compareTo(Marker o)
    {
        checkTypeCompatibility(o);
        if (isUpperUnbounded()) {
            return o.isUpperUnbounded() ? 0 : 1;
        }
        if (isLowerUnbounded()) {
            return o.isLowerUnbounded() ? 0 : -1;
        }
        if (o.isUpperUnbounded()) {
            return -1;
        }
        if (o.isLowerUnbounded()) {
            return 1;
        }
        // INVARIANT: value and o.value are present

        int compare = compare(valueBlock.get(), type, o.valueBlock.get(), o.getType());
        if (compare == 0) {
            if (bound == o.bound) {
                return 0;
            }
            if (bound == Bound.BELOW) {
                return -1;
            }
            if (bound == Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o.bound == Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }

    public int compare(Object left, Type ltype, Object right, Type rtype)
    {
        Type type = Utils.getCompatibleType(ltype, rtype);
        if (type.getPrimitiveType() == PrimitiveType.TINYINT) {
            long value1 = (long)left;
            long value2 = (long)right;
            return Long.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.SMALLINT) {
            long value1 = (long)left;
            long value2 = (long)right;
            return Long.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.INT) {
            long value1 = (long)left;
            long value2 = (long)right;
            return Long.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.BIGINT) {
            long value1 = (long)left;
            long value2 = (long)right;
            return Long.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.LARGEINT) {
            BigInteger value1 = null;
            BigInteger value2 = null;
           if (left instanceof BigInteger) {
               value1 = (BigInteger) left;
           } else {
               value1 = BigInteger.valueOf(((long)left));
           }
           if (right instanceof BigInteger) {
               value2 = (BigInteger) right;
           } else {
               value2 = BigInteger.valueOf(((long)right));
           }
            return value1.compareTo(value2);
        } else if (type.getPrimitiveType() == PrimitiveType.CHAR) {
            String value1 = (String) left;
            String value2 = (String) right;
            return value1.compareTo(value2);
        } else if (type.getPrimitiveType() == PrimitiveType.VARCHAR) {
            String value1 = (String) left;
            String value2 = (String) right;
            return value1.compareTo(value2);
        } else if (type.getPrimitiveType() == PrimitiveType.FLOAT) {
            float value1 = (float) left;
            float value2 = (float) right;
            return Float.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.DOUBLE) {
            double value1 = (double) left;
            double value2 = (double) right;
            return Double.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.BOOLEAN) {
            boolean value1 = (boolean) left;
            boolean value2 = (boolean) right;
            return Boolean.compare(value1, value2);
        } else if (type.getPrimitiveType() == PrimitiveType.DATE) {
            Date value1, value2;
            if (left instanceof Date) {
                value1 = (Date) left;
            } else {
                try {
                    value1 = new SimpleDateFormat("yyyy-MM-dd").parse((String)left);
                } catch (ParseException e) {
                    throw new RuntimeException("can not parse date type:"+left);
                }
            }
            if (right instanceof Date) {
                value2 = (Date) right;
            } else {
                try {
                    value2 = new SimpleDateFormat("yyyy-MM-dd").parse((String)right);
                } catch (ParseException e) {
                    throw new RuntimeException("can not parse date type:"+right);
                }
            }
            return value1.compareTo(value2);
        } else if (type.getPrimitiveType() == PrimitiveType.DATETIME) {
            Date value1, value2;
            if (left instanceof Date) {
                value1 = (Date) left;
            } else {
                try {
                    value1 = new SimpleDateFormat("yyyy-MM-dd").parse((String)left);
                } catch (ParseException e) {
                    throw new RuntimeException("can not parse date type:"+left);
                }
            }
            if (right instanceof Date) {
                value2 = (Date) right;
            } else {
                try {
                    value2 = new SimpleDateFormat("yyyy-MM-dd").parse((String)right);
                } catch (ParseException e) {
                    throw new RuntimeException("can not parse date type:"+right);
                }
            }
            return value1.compareTo(value2);
        } else if (type.getPrimitiveType() == PrimitiveType.DECIMAL) {
            BigDecimal value1 = (BigDecimal) left;
            BigDecimal value2 = (BigDecimal) right;
            return value1.compareTo(value2);
        } else if (type.getPrimitiveType() == PrimitiveType.DECIMALV2) {
            BigDecimal value1 = (BigDecimal) left;
            BigDecimal value2 = (BigDecimal) right;
            return value1.compareTo(value2);
        }
        return 1;
    }

    public static Marker min(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
    }

    public static Marker max(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", Marker.class.getSimpleName() + "[", "]");
        if (isLowerUnbounded()) {
            stringJoiner.add("lower unbounded");
        }
        else if (isUpperUnbounded()) {
            stringJoiner.add("upper unbounded");
        }
        stringJoiner.add("bound=" + bound);
        return stringJoiner.toString();
    }
}
