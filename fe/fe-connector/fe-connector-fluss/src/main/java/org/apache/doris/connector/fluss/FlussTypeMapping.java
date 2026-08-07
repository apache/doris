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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.ConnectorType;

import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeVisitor;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps a fluss {@link DataType} to a Doris {@link ConnectorType}.
 *
 * <p><b>Why a visitor and not a switch.</b> Fluss's {@link DataTypeVisitor} has one method per type,
 * so a type added by a future fluss release breaks this compile instead of falling into a
 * {@code default:} branch and degrading a column silently.
 *
 * <p><b>The parity invariant.</b> A datalake-enabled fluss table is reachable through two doors: the
 * table itself (this mapping) and its lake sibling {@code tbl$lake}, which is served by the paimon
 * connector over the paimon table fluss's tiering service writes. Both doors must describe the same
 * column the same way, so every rule below is the composition of fluss's own
 * {@code FlussDataTypeToPaimonDataType} with the paimon connector's {@code PaimonTypeMapping} —
 * including the ones that look arbitrary in isolation (CHAR over 255 collapsing to STRING, the
 * microsecond clamp, the two mapping switches). Changing a rule here without checking that
 * composition makes one table show two schemas.
 *
 * <p><b>Unsupported types degrade, they do not throw.</b> A type Doris cannot represent becomes the
 * {@code UNSUPPORTED} marker, which fe-core turns into {@code Type.UNSUPPORTED}: the table still
 * loads and still answers queries over its other columns, and only a query that projects that column
 * fails. Throwing here would make one exotic column cost the whole table.
 */
public final class FlussTypeMapping implements DataTypeVisitor<ConnectorType> {

    /** Doris DATETIMEV2 stops at microseconds; fluss timestamps go to nanoseconds (precision 9). */
    private static final int MAX_TIMESTAMP_SCALE = 6;

    /** Doris CHAR stops at 255 characters; a longer fluss CHAR becomes STRING. */
    private static final int MAX_CHAR_LENGTH = 255;

    /** The type name fe-core's converter turns into {@code Type.UNSUPPORTED}. */
    private static final String UNSUPPORTED = "UNSUPPORTED";

    private final Options options;

    private FlussTypeMapping(Options options) {
        this.options = options;
    }

    /** Convert one fluss type, top level or nested, under the given catalog mapping switches. */
    public static ConnectorType toConnectorType(DataType dataType, Options options) {
        return dataType.accept(new FlussTypeMapping(options));
    }

    @Override
    public ConnectorType visit(CharType charType) {
        int length = charType.getLength();
        if (length > MAX_CHAR_LENGTH) {
            return ConnectorType.of("STRING");
        }
        return ConnectorType.of("CHAR", length, 0);
    }

    @Override
    public ConnectorType visit(StringType stringType) {
        // Fluss STRING carries no declared length, so there is no VARCHAR(n) to preserve.
        return ConnectorType.of("STRING");
    }

    @Override
    public ConnectorType visit(BooleanType booleanType) {
        return ConnectorType.of("BOOLEAN");
    }

    @Override
    public ConnectorType visit(BinaryType binaryType) {
        if (options.isMapBinaryToVarbinary()) {
            // Doris has no fixed-length binary; the declared length survives as the VARBINARY bound.
            return ConnectorType.of("VARBINARY", binaryType.getLength(), 0);
        }
        return ConnectorType.of("STRING");
    }

    @Override
    public ConnectorType visit(BytesType bytesType) {
        if (options.isMapBinaryToVarbinary()) {
            // Unbounded in fluss: leave the length unset so fe-core's converter fills in the Doris
            // VARBINARY maximum, which is what the paimon side (BYTES = VARBINARY(Integer.MAX_VALUE))
            // resolves to as well.
            return ConnectorType.of("VARBINARY");
        }
        return ConnectorType.of("STRING");
    }

    @Override
    public ConnectorType visit(DecimalType decimalType) {
        return ConnectorType.of("DECIMALV3", decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public ConnectorType visit(TinyIntType tinyIntType) {
        return ConnectorType.of("TINYINT");
    }

    @Override
    public ConnectorType visit(SmallIntType smallIntType) {
        return ConnectorType.of("SMALLINT");
    }

    @Override
    public ConnectorType visit(IntType intType) {
        return ConnectorType.of("INT");
    }

    @Override
    public ConnectorType visit(BigIntType bigIntType) {
        return ConnectorType.of("BIGINT");
    }

    @Override
    public ConnectorType visit(FloatType floatType) {
        return ConnectorType.of("FLOAT");
    }

    @Override
    public ConnectorType visit(DoubleType doubleType) {
        return ConnectorType.of("DOUBLE");
    }

    @Override
    public ConnectorType visit(DateType dateType) {
        return ConnectorType.of("DATEV2");
    }

    @Override
    public ConnectorType visit(TimeType timeType) {
        // Doris has no storable TIME column type. Mapping it to STRING or to the elapsed-millis INT
        // that other engines use would hand back a value whose meaning differs from the source, so the
        // column is marked unsupported instead. The paimon and iceberg connectors mark their own TIME
        // the same way, which also keeps tbl and tbl$lake agreeing.
        return ConnectorType.of(UNSUPPORTED);
    }

    @Override
    public ConnectorType visit(TimestampType timestampType) {
        return ConnectorType.of("DATETIMEV2", clampScale(timestampType.getPrecision()), 0);
    }

    @Override
    public ConnectorType visit(LocalZonedTimestampType localZonedTimestampType) {
        int scale = clampScale(localZonedTimestampType.getPrecision());
        if (options.isMapTimestampTz()) {
            return ConnectorType.of("TIMESTAMPTZ", scale, 0);
        }
        return ConnectorType.of("DATETIMEV2", scale, 0);
    }

    @Override
    public ConnectorType visit(ArrayType arrayType) {
        // Element nullability is deliberately not carried: the paimon connector's read path leaves it
        // at the default, and the two must not disagree (see the parity invariant).
        return ConnectorType.arrayOf(arrayType.getElementType().accept(this));
    }

    @Override
    public ConnectorType visit(MapType mapType) {
        // MAP is a first-class fluss type on every path Doris uses: the ARROW log format, the compacted
        // KV format behind primary-key tables, and the paimon table the tiering service writes. Marking
        // it unsupported here would leave tbl$lake showing a MAP that tbl refuses to project.
        return ConnectorType.mapOf(
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public ConnectorType visit(RowType rowType) {
        List<DataField> fields = rowType.getFields();
        List<String> names = new ArrayList<>(fields.size());
        List<ConnectorType> types = new ArrayList<>(fields.size());
        List<Boolean> nullable = new ArrayList<>(fields.size());
        List<String> comments = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            names.add(field.getName());
            types.add(field.getType().accept(this));
            // Nested NOT NULL and COMMENT are carried so DESCRIBE / SHOW CREATE TABLE report what the
            // fluss schema actually declares for the field.
            nullable.add(field.getType().isNullable());
            comments.add(field.getDescription().orElse(null));
        }
        return ConnectorType.structOf(names, types, nullable, comments);
    }

    private static int clampScale(int precision) {
        return Math.min(precision, MAX_TIMESTAMP_SCALE);
    }

    /**
     * The catalog-level switches that change a mapping. Both default to off and both carry the same
     * name and meaning they have on other Doris catalogs (see {@link FlussCatalogProperties}).
     */
    public static final class Options {

        public static final Options DEFAULT = new Options(false, false);

        private final boolean mapBinaryToVarbinary;
        private final boolean mapTimestampTz;

        public Options(boolean mapBinaryToVarbinary, boolean mapTimestampTz) {
            this.mapBinaryToVarbinary = mapBinaryToVarbinary;
            this.mapTimestampTz = mapTimestampTz;
        }

        public boolean isMapBinaryToVarbinary() {
            return mapBinaryToVarbinary;
        }

        public boolean isMapTimestampTz() {
            return mapTimestampTz;
        }
    }
}
