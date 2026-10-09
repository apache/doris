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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Replace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

/** Applies connector-declared textual input semantics before ordinary sink type coercion. */
public final class ConnectorWriteValueConverter {
    private ConnectorWriteValueConverter() {
    }

    public static Expression convert(Column column, Expression input) {
        if (column.getConnectorStringWriteType() == null || !input.getDataType().isStringLikeType()) {
            return input;
        }
        DataType semanticType = DataType.fromCatalogType(column.getConnectorStringWriteType());
        DataType targetType = DataType.fromCatalogType(column.getType());
        if (!(semanticType instanceof UuidType) || !(targetType instanceof VarBinaryType)) {
            throw new AnalysisException("Unsupported connector string write conversion: "
                    + semanticType + " to " + targetType);
        }
        if (input instanceof StringLikeLiteral) {
            UUID uuid = new UuidLiteral(((StringLikeLiteral) input).getStringValue()).getValue();
            return new VarBinaryLiteral((VarBinaryType) targetType, ByteBuffer.allocate(16)
                    .putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
        }
        // Validate UUID text strictly before decoding canonical hex. A raw string-to-binary cast
        // would write 36 text bytes, and a permissive UUID cast would silently turn bad input into NULL.
        Expression uuidText = new Cast(new Cast(input, semanticType, false, true), StringType.INSTANCE);
        Expression hex = TypeCoercionUtils.processBoundFunction(
                new Replace(uuidText, new StringLiteral("-"), new StringLiteral("")));
        Expression bytes = TypeCoercionUtils.processBoundFunction(new Unhex(hex));
        return new Cast(bytes, targetType);
    }
}
