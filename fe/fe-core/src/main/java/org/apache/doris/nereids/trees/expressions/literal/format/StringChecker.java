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

import java.util.Objects;

/** StringChecker */
public class StringChecker extends FormatChecker {
    private final String str;

    public StringChecker(String name, String str) {
        super(name);
        this.str = Objects.requireNonNull(str, "str can not be null");
    }

    @Override
    protected boolean doCheck(StringInspect stringInspect) {
        if (stringInspect.remain() < str.length()) {
            return false;
        }

        for (int i = 0; i < str.length(); i++) {
            if (stringInspect.lookAndStep() != str.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
