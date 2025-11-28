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

/**
 * Time literal format checker, support two types of time string:
 *   colon format: ([+-])?\d+:\d{1,2}(:\d{1,2}(.\d+)?)?
 * NOTICE: only process colon format, because we do not treat numeric format as a time type
 *   when do string literal corecion
 */
public class TimeChecker extends FormatChecker {
    private static final TimeChecker INSTANCE = new TimeChecker();

    private final FormatChecker checker;

    private TimeChecker() {
        super("TimeChecker");

        this.checker =
                // time
                and("time format",
                    option("sign", or(ch('-'), ch('+'))),
                    // colon-format
                    and("colon format",
                        digit(1), // hour
                        ch(':'),
                        digit(1, 2), // minute
                        option("second and micro second",
                            and(
                                ch(':'),
                                digit(1, 2),
                                option("micro second", nanoSecond())
                            )
                        ) // second
                    )
                );
    }

    public static boolean isValidTime(String str) {
        str = str.trim();
        StringInspect stringInspect = new StringInspect(str.trim());
        return INSTANCE.check(stringInspect).matched && stringInspect.eos();
    }

    @Override
    protected boolean doCheck(StringInspect stringInspect) {
        return checker.check(stringInspect).matched;
    }

    private FormatChecker nanoSecond() {
        return and(
            ch('.'),
            digit(1)
        );
    }
}
