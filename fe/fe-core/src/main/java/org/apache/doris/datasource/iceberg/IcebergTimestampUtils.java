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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Nanosecond conversion and retained DATE-metric safeguards shared by the 4.2 planners. */
public final class IcebergTimestampUtils {
    private IcebergTimestampUtils() {
    }

    public static long parseNanos(String value, ZoneId zone) {
        // Static partitions must accept and round exactly the same literals as the sink cast.
        LocalDateTime timestamp = TimeStampNsLiteral.parseDateTime(value, zone);
        // Widen before combining: the minimum timestamp's whole second overflows long nanos.
        return BigDecimal.valueOf(timestamp.toEpochSecond(ZoneOffset.UTC))
                .multiply(BigDecimal.valueOf(1_000_000_000L))
                .add(BigDecimal.valueOf(timestamp.getNano())).longValueExact();
    }

    public static Set<Integer> unsafeNanosFields(Table table, Schema schema) {
        Set<Integer> unsafe = new HashSet<>();
        Map<Integer, Schema> history = table.schemas();
        int maxId = history.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1);
        // Removed history can leave live DATE files whose four-byte bounds Iceberg reads as longs.
        boolean incomplete = history.isEmpty() || history.size() != (long) maxId + 1;
        for (Types.NestedField field : TypeUtil.indexById(schema.asStruct()).values()) {
            if (field.type().typeId() != Type.TypeID.TIMESTAMP_NANO) {
                continue;
            }
            if (incomplete || history.values().stream().anyMatch(old -> {
                Type type = old.findType(field.fieldId());
                return type != null && type.typeId() == Type.TypeID.DATE;
            })) {
                unsafe.add(field.fieldId());
            }
        }
        return unsafe;
    }

    private static boolean unsafeName(String name, Schema schema, Set<Integer> unsafe) {
        Types.NestedField field = schema.caseInsensitiveFindField(name);
        return field != null && unsafe.contains(field.fieldId());
    }

    public static boolean referencesUnsafe(Expr expr, Schema schema, Set<Integer> unsafe) {
        if (expr instanceof SlotRef && unsafeName(((SlotRef) expr).getColumnName(), schema, unsafe)) {
            return true;
        }
        return expr.getChildren().stream().anyMatch(child -> referencesUnsafe(child, schema, unsafe));
    }

    public static boolean referencesUnsafe(org.apache.doris.nereids.trees.expressions.Expression expr,
            Schema schema, Set<Integer> unsafe) {
        return expr.getInputSlots().stream().anyMatch(slot -> unsafeName(slot.getName(), schema, unsafe));
    }
}
