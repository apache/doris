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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeV2Type;

import java.util.List;
import java.util.Optional;

/**
 * Temporal common-type promotion and detection of mixed temporal groups.
 * Implicit argument conversion belongs to {@link TypeCoercionUtils}; overload admission
 * belongs to SearchSignature. Conversion properties remain in {@link TemporalConversionProperties}.
 */
public final class TemporalTypeCoercion {
    private static final int HAS_TIMESTAMP_NS = 1;
    private static final int HAS_OTHER_DATE_LIKE = 2;
    private static final int HAS_BOTH = HAS_TIMESTAMP_NS | HAS_OTHER_DATE_LIKE;

    private TemporalTypeCoercion() {
    }

    /** Detect both temporal families in one traversal, including nested types. */
    public static boolean containsMixedTimeStampNsDateLikeTypes(List<DataType> dataTypes) {
        int found = 0;
        for (DataType dataType : dataTypes) {
            found = collectTemporalFamilies(dataType, found);
            if (found == HAS_BOTH) {
                return true;
            }
        }
        return false;
    }

    private static int collectTemporalFamilies(DataType dataType, int found) {
        if (found == HAS_BOTH) {
            return found;
        }
        if (dataType instanceof ArrayType) {
            return collectTemporalFamilies(((ArrayType) dataType).getItemType(), found);
        } else if (dataType instanceof MapType) {
            MapType map = (MapType) dataType;
            return collectTemporalFamilies(map.getValueType(), collectTemporalFamilies(map.getKeyType(), found));
        } else if (dataType instanceof StructType) {
            for (StructField field : ((StructType) dataType).getFields()) {
                found = collectTemporalFamilies(field.getDataType(), found);
                if (found == HAS_BOTH) {
                    break;
                }
            }
            return found;
        } else if (dataType.isTimeStampNsType()) {
            return found | HAS_TIMESTAMP_NS;
        } else if (dataType.isDateLikeType()) {
            return found | HAS_OTHER_DATE_LIKE;
        }
        return found;
    }

    /** Choose the common type when one operand is TIMESTAMP_NS. */
    public static Optional<DataType> getCommonDataTypeWithTimeStampNsType(DataType otherType) {
        if (otherType.isDateLikeType() || otherType.isNumericType()
                || otherType instanceof TimeV2Type || otherType.isStringLikeType()) {
            return Optional.of(TimeStampNsType.INSTANCE);
        }
        return Optional.empty();
    }

    /** Whether a comparison must preserve NS precision and check the temporal peer for overflow. */
    public static boolean isTimeStampNsAndDateLikePair(DataType left, DataType right) {
        if (left.isTimeStampNsType()) {
            return isOtherDateLikeType(right);
        }
        return right.isTimeStampNsType() && left.isDateLikeType();
    }

    /** A date-like type other than the fixed-range nanosecond representation. */
    private static boolean isOtherDateLikeType(DataType dataType) {
        return dataType.isDateLikeType() && !dataType.isTimeStampNsType();
    }

    /** Recognize an all-temporal NS group before order-sensitive legacy common-type reduction. */
    public static boolean containsTimeStampNsAndOnlyCompatibleTemporalTypes(List<DataType> dataTypes) {
        boolean containsTimeStampNs = false;
        for (DataType dataType : dataTypes) {
            if (dataType.isTimeStampNsType()) {
                containsTimeStampNs = true;
            } else if (!dataType.isDateLikeType() && !dataType.isNullType() && !(dataType instanceof TimeV2Type)) {
                return false;
            }
        }
        return containsTimeStampNs;
    }
}
