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

import java.util.function.Predicate;

/** FormatChecker */
public abstract class FormatChecker {
    protected final StringInspect stringInspect;
    protected int checkStartIndex;
    protected int checkEndIndex;

    public FormatChecker(StringInspect stringInspect) {
        this.stringInspect = stringInspect;
    }

    protected abstract boolean doCheck();

    public final boolean check() {
        this.checkStartIndex = stringInspect.index();
        boolean valid = doCheck();
        this.checkEndIndex = stringInspect.index();
        if (!valid) {
            stepBack();
        }
        return valid;
    }

    public void stepBack() {
        this.stringInspect.setIndex(checkStartIndex);
    }

    public String getCheckContent() {
        return stringInspect.str.substring(checkStartIndex, checkEndIndex);
    }

    protected OptionChecker option(FormatChecker checker) {
        return new OptionChecker(stringInspect, checker);
    }

    protected AndChecker and(FormatChecker... checkers) {
        return new AndChecker(stringInspect, Utils.fastToImmutableList(checkers));
    }

    protected OrChecker or(FormatChecker... checkers) {
        return new OrChecker(stringInspect, Utils.fastToImmutableList(checkers));
    }

    protected AtLeastChecker atLeast(int minCount, Predicate<Character> checker) {
        return new AtLeastChecker(stringInspect, minCount, -1, checker);
    }

    protected AtLeastChecker atLeast(int minCount, int maxRead, Predicate<Character> checker) {
        return new AtLeastChecker(stringInspect, minCount, maxRead, checker);
    }

    protected NumberChecker number(int minCount) {
        return new NumberChecker(stringInspect, minCount, -1);
    }

    protected NumberChecker number(int minCount, int maxRead) {
        return new NumberChecker(stringInspect, minCount, maxRead);
    }

    protected CharChecker ch(char c) {
        return new CharChecker(stringInspect, c);
    }

    protected CustomCharChecker chars(Predicate<Character> checker) {
        return new CustomCharChecker(stringInspect, checker);
    }

    protected StringChecker string(String equalsTo) {
        return new StringChecker(stringInspect, equalsTo);
    }
}
