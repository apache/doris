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

package org.apache.doris.nereids.trees.expressions.literal.format;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Util class for float/double to string.
 */
public class FractionalFormat {

    /**
     * Get string of double/float value for cast to string and output to mysql.
     *
     * @param value The double/float value.
     * @param precision precision
     * @param sciFormat format for string with scientific form.
     * @return string value.
     */
    public static String getFormatStringValue(double value, int precision, String sciFormat) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "Infinity" : "-Infinity";
        }
        if (Double.compare(value, 0.0) == 0) {
            return "0";
        }
        if (Double.compare(value, -0.0) == 0) {
            return "-0";
        }
        int expLower = -4;
        int exponent = (int) Math.floor(Math.log10(Math.abs(value)));
        if (exponent < precision && exponent >= expLower) {
            BigDecimal bd = new BigDecimal(value);
            bd = bd.setScale(precision - bd.precision() + bd.scale(), RoundingMode.HALF_UP);
            String result = bd.toPlainString();
            if (result.contains(".")) {
                result = result.replaceAll("0+$", "");
                if (result.endsWith(".")) {
                    result = result.substring(0, result.length() - 1);
                }
            }
            return result;
        } else {
            return String.format(sciFormat, value).replaceAll("(\\.\\d*?[1-9])0*E", "$1E")
                    .replaceAll("\\.0*E", "E").replaceAll("E", "e");
        }
    }
}
