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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TypeChange;

import java.util.Set;
import java.util.TreeSet;

/** Fail before planning files when a table requires semantics the native reader has not validated. */
final class DeltaLakeFeatureCheck {
    // Features are enabled alongside their end-to-end read coverage.
    static final Set<String> SUPPORTED_READER_FEATURES = Set.of(
            "columnMapping", "timestampNtz", "typeWidening", "typeWidening-preview", "deletionVectors");

    private DeltaLakeFeatureCheck() {
    }

    static void check(Snapshot snapshot) {
        check(snapshot.getPath(), DeltaKernelCompat.readerFeatures(snapshot));
        checkWidening(snapshot.getSchema(), snapshot.getPath());
    }

    static void check(String table, Set<String> readerFeatures) {
        Set<String> unsupported = new TreeSet<>(readerFeatures);
        unsupported.removeAll(SUPPORTED_READER_FEATURES);
        if (!unsupported.isEmpty()) {
            throw new DorisConnectorException("Unsupported Delta Lake reader features " + unsupported
                    + " for table " + table);
        }
    }

    static void checkWidening(StructType schema, String path) {
        for (StructField field : schema.fields()) {
            checkFieldWidening(field, path + "." + field.getName());
        }
    }

    private static void checkFieldWidening(StructField field, String path) {
        for (TypeChange change : field.getTypeChanges()) {
            if (!supportedWidening(change.getFrom(), change.getTo())) {
                throw new DorisConnectorException("Unsupported Delta Lake type widening for " + path
                        + ": " + change.getFrom() + " -> " + change.getTo());
            }
        }
        DataType type = field.getDataType();
        if (type instanceof StructType) {
            checkWidening((StructType) type, path);
        } else if (type instanceof ArrayType) {
            checkFieldWidening(((ArrayType) type).getElementField(), path + ".element");
        } else if (type instanceof MapType) {
            checkFieldWidening(((MapType) type).getKeyField(), path + ".key");
            checkFieldWidening(((MapType) type).getValueField(), path + ".value");
        }
    }

    static boolean supportedWidening(DataType from, DataType to) {
        int fromWidth = integerWidth(from);
        int toWidth = integerWidth(to);
        if (fromWidth > 0 && toWidth > fromWidth) {
            return true;
        }
        if (fromWidth > 0) {
            if (fromWidth <= 32 && to instanceof DoubleType) {
                return true;
            }
            if (to instanceof DecimalType) {
                DecimalType decimal = (DecimalType) to;
                int digits = fromWidth == 8 ? 3 : fromWidth == 16 ? 5 : fromWidth == 32 ? 10 : 19;
                return decimal.getPrecision() - decimal.getScale() >= digits;
            }
        }
        if (from instanceof FloatType && to instanceof DoubleType) {
            return true;
        }
        if (from instanceof DateType && to instanceof TimestampNTZType) {
            return true;
        }
        if (from instanceof DecimalType && to instanceof DecimalType) {
            DecimalType oldType = (DecimalType) from;
            DecimalType newType = (DecimalType) to;
            return newType.getScale() >= oldType.getScale()
                    && newType.getPrecision() - newType.getScale() >= oldType.getPrecision() - oldType.getScale();
        }
        return false;
    }

    private static int integerWidth(DataType type) {
        if (type instanceof ByteType) {
            return 8;
        }
        if (type instanceof ShortType) {
            return 16;
        }
        if (type instanceof IntegerType) {
            return 32;
        }
        return type instanceof LongType ? 64 : 0;
    }

}
