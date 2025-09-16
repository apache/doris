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

import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;

/** OrChecker */
public class OrChecker extends FormatChecker {
    private final List<FormatChecker> checkers;

    public OrChecker(String name, List<FormatChecker> checkers) {
        super(name);
        this.checkers = Utils.fastToImmutableList(
                Objects.requireNonNull(checkers, "checkers can not be null")
        );
    }

    @Override
    protected boolean doCheck(StringInspect stringInspect) {
        int maxMatches = -1;
        CheckResult maxMatchesResult = null;
        for (FormatChecker checker : checkers) {
            CheckResult checkResult = checker.check(stringInspect);
            if (checkResult.matched && checkResult.matchesLength() > maxMatches) {
                maxMatches = checkResult.matchesLength();
                maxMatchesResult = checkResult;
            }
            checkResult.stepBack();
        }
        if (maxMatches >= 0) {
            stringInspect.setIndex(maxMatchesResult.checkEndIndex);
            return true;
        } else {
            return false;
        }
    }
}
