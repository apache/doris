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

import java.util.Objects;
import java.util.function.Predicate;

/**
 * FormatChecker
 *
 * This class is used to check whether the string satisfy the underscore format(like DSL), without throw Exception.
 * For example, to check whether the string can be converted to integer, you can use below format, it can
 * check "1", "-123", and "-143". More complex example is DateTimeChecker
 *
 * <pre>
 *     FormatChecker checker = and(
 *       option(chars(c -> c == '+' || c == '-')),
 *       digit(1)
 *     );
 *
 *     checker.check(new StringInspector("+12345"))
 * </pre>
 */
public abstract class FormatChecker {
    public final String name;

    public FormatChecker(String name) {
        this.name = Objects.requireNonNull(name, "name can not be null");
    }

    protected abstract boolean doCheck(StringInspect stringInspect);

    /** check */
    public final CheckResult check(StringInspect stringInspect) {
        int checkStartIndex = stringInspect.index();
        boolean matches = doCheck(stringInspect);
        int checkEndIndex = stringInspect.index();
        CheckResult checkResult = new CheckResult(this, stringInspect, matches, checkStartIndex, checkEndIndex);
        if (!matches) {
            checkResult.stepBack();
        }
        return checkResult;
    }

    protected <T extends FormatChecker> DebugChecker<T> debug(T childChecker, Predicate<T> debugPoint) {
        return debug("DebugChecker", childChecker, debugPoint);
    }

    protected <T extends FormatChecker> DebugChecker<T> debug(String name, T childChecker, Predicate<T> debugPoint) {
        return new DebugChecker<>(name, childChecker, debugPoint);
    }

    protected OptionChecker option(FormatChecker checker) {
        return option("OptionChecker", checker);
    }

    protected OptionChecker option(String name, FormatChecker checker) {
        return new OptionChecker(name, checker);
    }

    protected AndChecker and(FormatChecker... checkers) {
        return and("AndChecker", checkers);
    }

    protected AndChecker and(String name, FormatChecker... checkers) {
        return new AndChecker(name, Utils.fastToImmutableList(checkers));
    }

    protected OrChecker or(FormatChecker... checkers) {
        return or("OrChecker", checkers);
    }

    protected OrChecker or(String name, FormatChecker... checkers) {
        return new OrChecker(name, Utils.fastToImmutableList(checkers));
    }

    protected AtLeastChecker atLeast(int minCount, Predicate<Character> checker) {
        return atLeast("AtLeastChecker", minCount, -1, checker);
    }

    protected AtLeastChecker atLeast(String name, int minCount, Predicate<Character> checker) {
        return new AtLeastChecker(name, minCount, -1, checker);
    }

    protected AtLeastChecker atLeast(int minCount, int maxRead, Predicate<Character> checker) {
        return atLeast("AtLeastChecker", minCount, maxRead, checker);
    }

    protected AtLeastChecker atLeast(String name, int minCount, int maxRead, Predicate<Character> checker) {
        return new AtLeastChecker(name, minCount, maxRead, checker);
    }

    protected DigitChecker digit(int minCount) {
        return digit("DigitChecker", minCount, -1);
    }

    protected DigitChecker digit(String name, int minCount) {
        return new DigitChecker(name, minCount, -1);
    }

    protected DigitChecker digit(int minCount, int maxRead) {
        return digit("DigitChecker", minCount, maxRead);
    }

    protected DigitChecker digit(String name, int minCount, int maxRead) {
        return new DigitChecker(name, minCount, maxRead);
    }

    protected LetterChecker letter(int minCount) {
        return letter("LetterChecker", minCount, -1);
    }

    protected LetterChecker letter(String name, int minCount) {
        return new LetterChecker(name, minCount, -1);
    }

    protected LetterChecker letter(int minCount, int maxRead) {
        return letter("LetterChecker", minCount, maxRead);
    }

    protected LetterChecker letter(String name, int minCount, int maxRead) {
        return new LetterChecker(name, minCount, maxRead);
    }

    protected CharChecker ch(char c) {
        return ch("CharChecker", c);
    }

    protected CharChecker ch(String name, char c) {
        return new CharChecker(name, c);
    }

    protected CustomCharChecker chars(Predicate<Character> checker) {
        return chars("CustomCharChecker", checker);
    }

    protected CustomCharChecker chars(String name, Predicate<Character> checker) {
        return new CustomCharChecker(name, checker);
    }

    protected StringChecker string(String equalsTo) {
        return string("StringChecker", equalsTo);
    }

    protected StringChecker string(String name, String equalsTo) {
        return new StringChecker(name, equalsTo);
    }

    @Override
    public String toString() {
        return name;
    }
}
