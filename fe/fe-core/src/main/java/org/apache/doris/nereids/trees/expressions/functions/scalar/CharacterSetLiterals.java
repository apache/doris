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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Character sets accepted by encode and decode. */
final class CharacterSetLiterals {
    private static final List<String> SUPPORTED = ImmutableList.of(
            "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16");

    private CharacterSetLiterals() {
    }

    static void checkSecondArgument(ScalarFunction function) {
        Expression characterSet = function.getArgument(1);
        if (!characterSet.isLiteral()) {
            throw new AnalysisException("the second argument of function "
                    + function.getName() + " must be a literal: " + function.toSql());
        }
        if (characterSet.isNullLiteral()) {
            return;
        }
        if (!(characterSet instanceof StringLikeLiteral)) {
            throw new AnalysisException("the second argument of function "
                    + function.getName() + " must be a string literal: " + function.toSql());
        }
        String value = ((StringLikeLiteral) characterSet).getValue();
        for (String supported : SUPPORTED) {
            if (equalsIgnoreAsciiCase(value, supported)) {
                return;
            }
        }
        throw new AnalysisException("Unsupported character set '" + value
                + "'. Supported character sets are US-ASCII, ISO-8859-1, UTF-8, "
                + "UTF-16BE, UTF-16LE, and UTF-16");
    }

    private static boolean equalsIgnoreAsciiCase(String value, String expected) {
        if (value.length() != expected.length()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current >= 'a' && current <= 'z') {
                current -= 'a' - 'A';
            }
            if (current != expected.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
