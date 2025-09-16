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

/** FloatChecker */
public class FloatChecker extends FormatChecker {
    private static final FloatChecker INSTANCE = new FloatChecker();
    private final FormatChecker checker;

    private FloatChecker() {
        super("FloatChecker");
        checker = and(
            option(chars(c -> c == '+' || c == '-')),
            or(
                // 123 or 123.456
                and(digit(1), option(and(ch('.'), digit(0)))),
                // .123
                and(ch('.'), digit(1))
            ),
            option(
                // E+10 or E-10 or E10
                and(
                    chars(c -> c == 'e' || c == 'E'),
                    option(chars(c -> c == '+' || c == '-')),
                    digit(1)
                )
            )
        );
    }

    public static boolean isValidFloat(String str) {
        StringInspect stringInspect = new StringInspect(str.trim());
        return INSTANCE.check(stringInspect).matched && stringInspect.eos();
    }

    @Override
    protected boolean doCheck(StringInspect stringInspect) {
        return checker.check(stringInspect).matched;
    }
}
