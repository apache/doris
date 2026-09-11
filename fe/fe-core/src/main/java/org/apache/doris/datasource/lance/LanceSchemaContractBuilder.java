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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.lance.job.LanceIndexSchemaContract;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.lance.schema.LanceField;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Builds schema contract v1 for one indexed column from the fresh LanceField tree of the
 * pinned admission snapshot. {@link LanceTypeConverter} is deliberately bypassed because it
 * erases fixed-size-list dimensions, float16-vs-float32, and timestamp timezones.
 *
 * <p>The input is the stored column name (byte-identical to the LanceField name), never the
 * raw user input: callers resolve it via the table's column lookup first. Matching is exact
 * and top-level only; a missing field fails closed. The builder makes no supportability
 * judgment — every ArrowType yields a deterministic canonical string — but the numeric facts
 * it copies into the contract (field id, fixed-size-list dimension) are validated: a negative
 * field id (0 is legal) or a non-positive dimension is a malformed provider fact and fails
 * closed the same way. The canonical vocabulary defined here is the Java-side authority the
 * Rust worker's golden fixtures align to (design section 4.2).
 *
 * <p>Fixed-size-list element facts are sourced from the reconstructed Arrow view, not from
 * the LanceField tree. The pinned SDK's {@code Dataset.getLanceSchema()} collapses the element
 * into the manifest logical-type string (for example {@code fixed_size_list:float:4}), so the
 * field's children are always empty, while {@link LanceField#asArrowField()} synthesizes the
 * element back as the first child of the Arrow view. The element type is recovered from that
 * synthesized child; its nullability is copied as synthesized — the manifest has no slot for
 * element nullability, so the reconstruction reports {@code true} today regardless of what was
 * written, and existing datasets are as affected as new ones. The contract therefore records
 * reconstructed-schema facts, which is exactly what the worker observes through the same SDK;
 * copying the synthesized flag instead of pinning {@code true} lets the contract improve
 * automatically if a future SDK preserves element nullability.
 */
final class LanceSchemaContractBuilder {
    /** Timezones that fit the canonical {@code tz="…"} slot without any escaping (IANA names). */
    private static final Pattern SAFE_TIMEZONE = Pattern.compile("[A-Za-z0-9+_/-]+");

    private LanceSchemaContractBuilder() {
    }

    /**
     * Builds the single-field contract for {@code storedColumnName}. Only top-level fields are
     * considered; nested subfields never enter the contract.
     */
    static LanceIndexSchemaContract build(List<LanceField> topLevelFields, String storedColumnName)
            throws AnalysisException {
        if (topLevelFields == null) {
            throw new IllegalArgumentException("Lance top-level schema fields must not be null");
        }
        if (storedColumnName == null || storedColumnName.isEmpty()) {
            throw new IllegalArgumentException("stored column name must not be null or empty");
        }
        for (LanceField field : topLevelFields) {
            if (field == null) {
                throw new IllegalArgumentException("Lance top-level schema field must not be null");
            }
            if (storedColumnName.equals(field.getName())) {
                return new LanceIndexSchemaContract(
                        Collections.singletonList(indexedField(field)));
            }
        }
        ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID,
                "unsupported schema contract: indexed field not found");
        throw new IllegalStateException("unreachable");
    }

    private static LanceIndexSchemaContract.IndexedField indexedField(LanceField field)
            throws AnalysisException {
        ArrowType type = field.getType();
        if (type == null) {
            throw new IllegalArgumentException("Lance field type must not be null");
        }
        long fieldId = field.getId();
        if (fieldId < 0) {
            // Field id 0 is a legal provider id; only negatives are malformed facts, and they
            // fail closed like a missing field — bounded error, no provider string echoed.
            ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID,
                    "unsupported schema contract: indexed field id must not be negative");
        }
        Integer fixedSizeListDimension = null;
        String vectorElementType = null;
        Boolean vectorElementNullable = null;
        if (type instanceof ArrowType.FixedSizeList) {
            int listSize = ((ArrowType.FixedSizeList) type).getListSize();
            if (listSize <= 0) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID,
                        "unsupported schema contract: fixed-size list dimension must be positive");
            }
            fixedSizeListDimension = listSize;
            // The pinned SDK collapses the element into the manifest logical-type string, so the
            // LanceField tree never carries children for a fixed-size list; asArrowField()
            // re-synthesizes the element as the first child of the reconstructed Arrow view. The
            // synthesized child's nullability is always true — the manifest has no slot for
            // element nullability — and it is copied as-is so the contract tracks whatever the
            // SDK can reconstruct rather than pinning the limitation.
            Field arrowView = field.asArrowField();
            List<Field> elements = arrowView == null ? null : arrowView.getChildren();
            if (elements == null || elements.isEmpty() || elements.get(0) == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_LANCE_INDEX_INVALID,
                        "unsupported schema contract: fixed-size list element must be present");
            }
            Field element = elements.get(0);
            vectorElementType = canonicalType(element.getType());
            vectorElementNullable = element.isNullable();
        }
        return new LanceIndexSchemaContract.IndexedField(
                fieldId, field.getName().toLowerCase(Locale.ROOT), canonicalType(type),
                field.isNullable(), fixedSizeListDimension, vectorElementType, vectorElementNullable);
    }

    /**
     * Canonical normalizedType. Fixed-size lists map to the pinned literal
     * {@code fixed_size_list} — the dimension and element facts live in their own contract
     * fields. Anything without a canonical row degrades to the generic recursive form
     * {@code <arrow class name lowercased>(param=value,...)} with parameters in ArrowType
     * declaration order.
     */
    private static String canonicalType(ArrowType type) {
        if (type == null) {
            throw new IllegalArgumentException("Lance field type must not be null");
        }
        if (type instanceof ArrowType.Bool) {
            return "bool";
        }
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            switch (intType.getBitWidth()) {
                case 8:
                case 16:
                case 32:
                case 64:
                    return (intType.getIsSigned() ? "int<" : "uint<") + intType.getBitWidth() + ">";
                default:
                    return genericType(type);
            }
        }
        if (type instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
            if (precision == FloatingPointPrecision.HALF) {
                return "float16";
            }
            if (precision == FloatingPointPrecision.SINGLE) {
                return "float32";
            }
            if (precision == FloatingPointPrecision.DOUBLE) {
                return "float64";
            }
            return genericType(type);
        }
        if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return "decimal<" + decimal.getBitWidth() + ">("
                    + decimal.getPrecision() + "," + decimal.getScale() + ")";
        }
        if (type instanceof ArrowType.Utf8) {
            return "utf8";
        }
        if (type instanceof ArrowType.LargeUtf8) {
            return "large_utf8";
        }
        if (type instanceof ArrowType.Date) {
            DateUnit unit = ((ArrowType.Date) type).getUnit();
            if (unit == DateUnit.DAY) {
                return "date<day>";
            }
            if (unit == DateUnit.MILLISECOND) {
                return "date<ms>";
            }
            return genericType(type);
        }
        if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
            String unit = canonicalTimestampUnit(timestamp.getUnit());
            String timezone = timestamp.getTimezone();
            if (unit != null && (timezone == null || SAFE_TIMEZONE.matcher(timezone).matches())) {
                return "timestamp<" + unit + ",tz=\"" + (timezone == null ? "" : timezone) + "\">";
            }
            return genericType(type);
        }
        if (type instanceof ArrowType.FixedSizeList) {
            return "fixed_size_list";
        }
        return genericType(type);
    }

    private static String canonicalTimestampUnit(TimeUnit unit) {
        if (unit == TimeUnit.SECOND) {
            return "sec";
        }
        if (unit == TimeUnit.MILLISECOND) {
            return "ms";
        }
        if (unit == TimeUnit.MICROSECOND) {
            return "us";
        }
        if (unit == TimeUnit.NANOSECOND) {
            return "ns";
        }
        return null;
    }

    private static String genericType(ArrowType type) {
        String name = type.getClass().getSimpleName().toLowerCase(Locale.ROOT);
        StringBuilder params = new StringBuilder();
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            params.append("bitWidth=").append(intType.getBitWidth())
                    .append(",signed=").append(intType.getIsSigned());
        } else if (type instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
            params.append("unit=").append(String.valueOf(timestamp.getUnit()))
                    .append(",timezone=").append(String.valueOf(timestamp.getTimezone()));
        } else if (type instanceof ArrowType.Time) {
            ArrowType.Time time = (ArrowType.Time) type;
            params.append("unit=").append(String.valueOf(time.getUnit()))
                    .append(",bitWidth=").append(time.getBitWidth());
        } else if (type instanceof ArrowType.Duration) {
            params.append("unit=").append(String.valueOf(((ArrowType.Duration) type).getUnit()));
        } else if (type instanceof ArrowType.Interval) {
            params.append("unit=").append(String.valueOf(((ArrowType.Interval) type).getUnit()));
        } else if (type instanceof ArrowType.Date) {
            params.append("unit=").append(String.valueOf(((ArrowType.Date) type).getUnit()));
        } else if (type instanceof ArrowType.FloatingPoint) {
            params.append("precision=")
                    .append(String.valueOf(((ArrowType.FloatingPoint) type).getPrecision()));
        } else if (type instanceof ArrowType.FixedSizeBinary) {
            params.append("byteWidth=").append(((ArrowType.FixedSizeBinary) type).getByteWidth());
        } else if (type instanceof ArrowType.Map) {
            params.append("keysSorted=").append(((ArrowType.Map) type).getKeysSorted());
        } else if (type instanceof ArrowType.Union) {
            ArrowType.Union union = (ArrowType.Union) type;
            params.append("mode=").append(String.valueOf(union.getMode()))
                    .append(",typeIds=").append(Arrays.toString(union.getTypeIds()));
        } else if (type instanceof ArrowType.ExtensionType) {
            ArrowType.ExtensionType extension = (ArrowType.ExtensionType) type;
            params.append("extensionName=").append(String.valueOf(extension.extensionName()))
                    .append(",storageType=").append(canonicalType(extension.storageType()));
        }
        return name + "(" + params + ")";
    }
}
