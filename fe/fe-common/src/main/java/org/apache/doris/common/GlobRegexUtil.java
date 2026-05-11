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

package org.apache.doris.common;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;
import dk.brics.automaton.BasicOperations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility to convert a restricted glob pattern into a regex.
 *
 * Supported glob syntax:
 * - '*' matches any sequence of characters
 * - '?' matches any single character
 * - '[...]' matches any character in the brackets
 * - '[!...]' matches any character not in the brackets
 * - '\\' escapes the next character
 */
public final class GlobRegexUtil {
    // Small LRU to cap compiled pattern memory.
    private static final int REGEX_CACHE_CAPACITY = 256;
    private static final Map<String, Pattern> REGEX_CACHE = new LinkedHashMap<String, Pattern>(
            REGEX_CACHE_CAPACITY, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Pattern> eldest) {
            return size() > REGEX_CACHE_CAPACITY;
        }
    };

    private GlobRegexUtil() {
    }

    public static Pattern getOrCompilePattern(String globPattern) {
        synchronized (REGEX_CACHE) {
            Pattern cached = REGEX_CACHE.get(globPattern);
            if (cached != null) {
                return cached;
            }
            String regex = globToRegex(globPattern);
            Pattern compiled;
            try {
                compiled = Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid glob pattern: " + globPattern, e);
            }
            REGEX_CACHE.put(globPattern, compiled);
            return compiled;
        }
    }

    public static boolean matches(String globPattern, String candidate) {
        return getOrCompilePattern(globPattern).matcher(candidate).matches();
    }

    public static boolean isGlobSubsetOf(String subsetGlobPattern, String supersetGlobPattern) {
        return globToAutomaton(subsetGlobPattern).subsetOf(globToAutomaton(supersetGlobPattern));
    }

    private static Automaton globToAutomaton(String pattern) {
        List<Automaton> automata = new ArrayList<>();
        boolean isEscaped = false;
        int patternLength = pattern.length();
        for (int index = 0; index < patternLength; index++) {
            char currentChar = pattern.charAt(index);
            if (isEscaped) {
                automata.add(BasicAutomata.makeChar(currentChar));
                isEscaped = false;
                continue;
            }
            if (currentChar == '\\') {
                isEscaped = true;
                continue;
            }
            if (currentChar == '*') {
                automata.add(BasicAutomata.makeAnyString());
                continue;
            }
            if (currentChar == '?') {
                automata.add(BasicAutomata.makeAnyChar());
                continue;
            }
            if (currentChar == '[') {
                CharacterClass characterClass = parseCharacterClass(pattern, index);
                automata.add(characterClass.toAutomaton(pattern));
                index = characterClass.closeIndex;
                continue;
            }
            automata.add(BasicAutomata.makeChar(currentChar));
        }
        if (isEscaped) {
            automata.add(BasicAutomata.makeChar('\\'));
        }
        if (automata.isEmpty()) {
            return BasicAutomata.makeEmptyString();
        }
        return BasicOperations.concatenate(automata);
    }

    public static String globToRegex(String pattern) {
        StringBuilder regexBuilder = new StringBuilder();
        regexBuilder.append("^");
        boolean isEscaped = false;
        int patternLength = pattern.length();
        for (int index = 0; index < patternLength; index++) {
            char currentChar = pattern.charAt(index);
            if (isEscaped) {
                appendEscapedRegexChar(regexBuilder, currentChar);
                isEscaped = false;
                continue;
            }
            if (currentChar == '\\') {
                isEscaped = true;
                continue;
            }
            if (currentChar == '*') {
                regexBuilder.append(".*");
                continue;
            }
            if (currentChar == '?') {
                regexBuilder.append('.');
                continue;
            }
            if (currentChar == '[') {
                CharacterClass characterClass = parseCharacterClass(pattern, index);
                regexBuilder.append('[');
                if (characterClass.negated) {
                    regexBuilder.append('^');
                }
                regexBuilder.append(characterClass.classChars).append(']');
                index = characterClass.closeIndex;
                continue;
            }
            appendEscapedRegexChar(regexBuilder, currentChar);
        }
        if (isEscaped) {
            appendEscapedRegexChar(regexBuilder, '\\');
        }
        regexBuilder.append("$");
        return regexBuilder.toString();
    }

    private static CharacterClass parseCharacterClass(String pattern, int openIndex) {
        int classIndex = openIndex + 1;
        boolean negated = false;
        if (classIndex < pattern.length()
                && (pattern.charAt(classIndex) == '!' || pattern.charAt(classIndex) == '^')) {
            negated = true;
            classIndex++;
        }
        boolean classClosed = false;
        boolean isClassEscaped = false;
        StringBuilder classBuffer = new StringBuilder();
        int classCharCount = 0;
        for (; classIndex < pattern.length(); classIndex++) {
            char classChar = pattern.charAt(classIndex);
            if (isClassEscaped) {
                classBuffer.append(classChar);
                isClassEscaped = false;
                classCharCount++;
                continue;
            }
            if (classChar == '\\') {
                isClassEscaped = true;
                continue;
            }
            if (classChar == ']') {
                classClosed = true;
                break;
            }
            classBuffer.append(classChar);
            classCharCount++;
        }
        if (!classClosed) {
            throw new IllegalArgumentException("Unclosed character class in glob pattern: " + pattern);
        }
        if (classCharCount == 0) {
            throw new IllegalArgumentException("Empty character class in glob pattern: " + pattern);
        }
        return new CharacterClass(classBuffer.toString(), negated, classIndex);
    }

    private static Automaton makeCharacterClassAutomaton(String classChars, boolean negated, String pattern) {
        List<Automaton> automata = new ArrayList<>();
        for (int index = 0; index < classChars.length(); index++) {
            char start = classChars.charAt(index);
            if (index + 2 < classChars.length() && classChars.charAt(index + 1) == '-') {
                char end = classChars.charAt(index + 2);
                if (start > end) {
                    throw new IllegalArgumentException("Invalid character range in glob pattern: " + pattern);
                }
                automata.add(BasicAutomata.makeCharRange(start, end));
                index += 2;
            } else {
                automata.add(BasicAutomata.makeChar(start));
            }
        }
        Automaton positive = automata.size() == 1 ? automata.get(0) : BasicOperations.union(automata);
        if (!negated) {
            return positive;
        }
        return BasicOperations.minus(BasicAutomata.makeAnyChar(), positive);
    }

    private static void appendEscapedRegexChar(StringBuilder regexBuilder, char ch) {
        switch (ch) {
            case '.':
            case '^':
            case '$':
            case '+':
            case '*':
            case '?':
            case '(':
            case ')':
            case '|':
            case '{':
            case '}':
            case '[':
            case ']':
            case '\\':
                regexBuilder.append('\\').append(ch);
                break;
            default:
                regexBuilder.append(ch);
                break;
        }
    }

    private static final class CharacterClass {
        private final String classChars;
        private final boolean negated;
        private final int closeIndex;

        private CharacterClass(String classChars, boolean negated, int closeIndex) {
            this.classChars = classChars;
            this.negated = negated;
            this.closeIndex = closeIndex;
        }

        private Automaton toAutomaton(String pattern) {
            return makeCharacterClassAutomaton(classChars, negated, pattern);
        }
    }
}
