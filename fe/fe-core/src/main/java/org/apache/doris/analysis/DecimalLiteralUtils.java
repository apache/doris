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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.SessionVariable;

import java.math.BigDecimal;

/**
 * Utility class for creating {@link DecimalLiteral} from string values.
 * This decouples DecimalLiteral construction logic from SessionVariable,
 * so that DecimalLiteral itself has no dependency on the session layer.
 */
public class DecimalLiteralUtils {

    /**
     * Create a DecimalLiteral from a string value, reading enableDecimal256
     * from the current session context.
     */
    public static DecimalLiteral create(String value) throws AnalysisException {
        return create(value, SessionVariable.getEnableDecimal256());
    }

    /**
     * Create a DecimalLiteral from a string value with an explicit enableDecimal256 flag.
     * This method has no dependency on SessionVariable or ConnectContext.
     *
     * <p>The logic determines the decimal type by:
     * <ol>
     *   <li>Parsing the string to a BigDecimal</li>
     *   <li>Computing precision and scale</li>
     *   <li>If precision exceeds maxPrecision (38 or 76), attempting to strip trailing zeros</li>
     *   <li>Creating a DecimalLiteral with the computed type</li>
     * </ol>
     */
    public static DecimalLiteral create(String value, boolean enableDecimal256) throws AnalysisException {
        BigDecimal v;
        try {
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        return create(v, enableDecimal256);
    }

    /**
     * Create a DecimalLiteral from a BigDecimal value with an explicit enableDecimal256 flag.
     */
    public static DecimalLiteral create(BigDecimal v, boolean enableDecimal256) {
        int precision = DecimalLiteral.getBigDecimalPrecision(v);
        int scale = DecimalLiteral.getBigDecimalScale(v);
        int maxPrecision = enableDecimal256
                ? ScalarType.MAX_DECIMAL256_PRECISION : ScalarType.MAX_DECIMAL128_PRECISION;
        int integerPart = precision - scale;
        if (precision > maxPrecision) {
            BigDecimal stripedValue = v.stripTrailingZeros();
            int stripedPrecision = DecimalLiteral.getBigDecimalPrecision(stripedValue);
            if (stripedPrecision <= maxPrecision) {
                v = stripedValue.setScale(maxPrecision - integerPart);
                precision = DecimalLiteral.getBigDecimalPrecision(v);
                scale = DecimalLiteral.getBigDecimalScale(v);
            }
        }
        ScalarType type = ScalarType.createDecimalType(precision, scale);
        return new DecimalLiteral(v, type);
    }
}
