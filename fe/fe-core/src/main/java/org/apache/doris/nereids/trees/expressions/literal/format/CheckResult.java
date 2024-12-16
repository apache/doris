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

/** CheckResult */
public class CheckResult {
    public final FormatChecker checker;
    public final StringInspect stringInspect;
    public final boolean matched;
    public final int checkStartIndex;
    public final int checkEndIndex;

    public CheckResult(
            FormatChecker checker, StringInspect stringInspect,
            boolean matched, int checkStartIndex, int checkEndIndex) {
        this.checker = checker;
        this.stringInspect = stringInspect;
        this.matched = matched;
        this.checkStartIndex = checkStartIndex;
        this.checkEndIndex = checkEndIndex;
    }

    public int matchesLength() {
        return checkEndIndex - checkStartIndex;
    }

    public String matchesContent() {
        return stringInspect.str.substring(checkStartIndex, checkEndIndex);
    }

    public void stepBack() {
        stringInspect.setIndex(checkStartIndex);
    }
}
