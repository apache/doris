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

package org.apache.doris.filesystem.spi;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provider-neutral glob helpers for object-storage keys.
 */
public final class ObjectStorageGlob {

    private static final int MAX_EXPANDED_GLOB_LIST_PREFIXES = 256;
    private static final Comparator<String> UTF8_BINARY_ORDER = ObjectStorageGlob::compareUtf8Binary;
    private static final Pattern NUMERIC_RANGE_PATTERN = Pattern.compile("-?\\d+\\.\\.-?\\d+");
    private static final Pattern NUMERIC_RANGE_ALTERNATIVE = Pattern.compile("(-?\\d+)\\.\\.(-?\\d+)");

    private ObjectStorageGlob() {
    }

    /**
     * Returns the longest key prefix that contains no glob metacharacters.
     */
    public static String longestNonGlobPrefix(String globPattern) {
        int earliest = globPattern.length();
        for (char c : new char[]{'*', '?', '[', '{', '\\'}) {
            int idx = globPattern.indexOf(c);
            if (idx >= 0 && idx < earliest) {
                earliest = idx;
            }
        }
        return globPattern.substring(0, earliest);
    }

    /**
     * Returns object-store list prefixes that are safe to push down for a glob pattern.
     *
     * <p>Unlike {@link #longestNonGlobPrefix(String)}, this expands bounded glob constructs
     * ({@code {...}} alternation and positive {@code [...]} character classes) before the first
     * unbounded wildcard. For example,
     * {@code date=2025-{0[3-9],1[0-2]}-01/mp_id=8/*} becomes concrete date prefixes instead
     * of one broad {@code date=2025-} scan. If expansion would be too large or unsafe, it
     * falls back to the conservative longest static prefix.
     */
    public static List<String> expandedGlobListPrefixes(String globPattern) {
        List<String> prefixes = expandGlobListPrefixes(globPattern, true);
        return prefixes == null ? List.of(longestNonGlobPrefix(globPattern)) : prefixes;
    }

    private static List<String> expandGlobListPrefixes(String globPattern, boolean allowPartialPrefix) {
        List<String> prefixes = new ArrayList<>();
        prefixes.add("");
        int i = 0;
        while (i < globPattern.length()) {
            char c = globPattern.charAt(i);
            if (c == '*' || c == '?') {
                return allowPartialPrefix ? compactPrefixes(prefixes) : null;
            }
            if (c == '\\') {
                if (i + 1 < globPattern.length()) {
                    appendLiteral(prefixes, globPattern.charAt(i + 1));
                    i += 2;
                } else {
                    appendLiteral(prefixes, c);
                    i++;
                }
                continue;
            }
            if (c == '[') {
                PrefixExpansion charClass = expandCharacterClass(globPattern, i);
                if (charClass == null) {
                    return allowPartialPrefix ? compactPrefixes(prefixes) : null;
                }
                prefixes = appendAlternatives(prefixes, charClass.values);
                if (prefixes == null) {
                    return null;
                }
                i = charClass.nextIndex;
                continue;
            }
            if (c == '{') {
                PrefixExpansion brace = expandBraceGroup(globPattern, i);
                if (brace == null) {
                    return allowPartialPrefix ? compactPrefixes(prefixes) : null;
                }
                prefixes = appendAlternatives(prefixes, brace.values);
                if (prefixes == null) {
                    return null;
                }
                i = brace.nextIndex;
                continue;
            }
            appendLiteral(prefixes, c);
            i++;
        }
        return compactPrefixes(prefixes);
    }

    private static void appendLiteral(List<String> prefixes, char c) {
        for (int i = 0; i < prefixes.size(); i++) {
            prefixes.set(i, prefixes.get(i) + c);
        }
    }

    private static List<String> appendAlternatives(List<String> prefixes, List<String> alternatives) {
        long expandedSize = (long) prefixes.size() * alternatives.size();
        if (expandedSize > MAX_EXPANDED_GLOB_LIST_PREFIXES) {
            return null;
        }
        List<String> expanded = new ArrayList<>((int) expandedSize);
        for (String prefix : prefixes) {
            for (String alternative : alternatives) {
                expanded.add(prefix + alternative);
            }
        }
        return expanded;
    }

    private static PrefixExpansion expandCharacterClass(String globPattern, int openIndex) {
        int closeIndex = findClosingBracket(globPattern, openIndex);
        if (closeIndex < 0 || closeIndex == openIndex + 1) {
            return null;
        }
        int i = openIndex + 1;
        char first = globPattern.charAt(i);
        if (first == '!' || first == '^') {
            return null;
        }
        if (containsSurrogate(globPattern, i, closeIndex)) {
            return null;
        }
        List<String> values = new ArrayList<>();
        while (i < closeIndex) {
            char current = globPattern.charAt(i);
            if (current == '\\') {
                if (i + 1 >= closeIndex) {
                    return null;
                }
                values.add(String.valueOf(globPattern.charAt(i + 1)));
                i += 2;
                continue;
            }
            if (i + 2 < closeIndex && globPattern.charAt(i + 1) == '-') {
                char rangeEnd = globPattern.charAt(i + 2);
                int step = current <= rangeEnd ? 1 : -1;
                for (char ch = current; step > 0 ? ch <= rangeEnd : ch >= rangeEnd; ch += step) {
                    values.add(String.valueOf(ch));
                    if (values.size() > MAX_EXPANDED_GLOB_LIST_PREFIXES) {
                        return null;
                    }
                }
                i += 3;
                continue;
            }
            values.add(String.valueOf(current));
            i++;
        }
        return new PrefixExpansion(values, closeIndex + 1);
    }

    private static int findClosingBracket(String globPattern, int openIndex) {
        for (int i = openIndex + 1; i < globPattern.length(); i++) {
            char c = globPattern.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (c == ']') {
                return i;
            }
        }
        return -1;
    }

    private static boolean containsSurrogate(String text, int start, int end) {
        for (int i = start; i < end; i++) {
            if (Character.isSurrogate(text.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static PrefixExpansion expandBraceGroup(String globPattern, int openIndex) {
        int closeIndex = findClosingBrace(globPattern, openIndex);
        if (closeIndex < 0) {
            return null;
        }
        List<String> alternatives = splitBraceAlternatives(
                globPattern.substring(openIndex + 1, closeIndex));
        if (alternatives.isEmpty()) {
            return null;
        }
        List<String> values = new ArrayList<>();
        for (String alternative : alternatives) {
            if (containsNumericRange(alternative)) {
                return null;
            }
            List<String> expandedAlternative = expandGlobListPrefixes(alternative, false);
            if (expandedAlternative == null) {
                return null;
            }
            values.addAll(expandedAlternative);
            if (values.size() > MAX_EXPANDED_GLOB_LIST_PREFIXES) {
                return null;
            }
        }
        return new PrefixExpansion(values, closeIndex + 1);
    }

    private static int findClosingBrace(String globPattern, int openIndex) {
        int depth = 0;
        for (int i = openIndex; i < globPattern.length(); i++) {
            char c = globPattern.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitBraceAlternatives(String content) {
        List<String> alternatives = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                if (depth == 0) {
                    return Collections.emptyList();
                }
                depth--;
            } else if (c == ',' && depth == 0) {
                alternatives.add(content.substring(start, i));
                start = i + 1;
            }
        }
        if (depth != 0) {
            return Collections.emptyList();
        }
        alternatives.add(content.substring(start));
        return alternatives;
    }

    private static List<String> compactPrefixes(List<String> prefixes) {
        List<String> sorted = new ArrayList<>(prefixes);
        sorted.sort(UTF8_BINARY_ORDER);
        List<String> compact = new ArrayList<>();
        for (String prefix : sorted) {
            if (prefix.isEmpty()) {
                return List.of("");
            }
            boolean covered = false;
            for (String existing : compact) {
                if (prefix.startsWith(existing)) {
                    covered = true;
                    break;
                }
            }
            if (!covered) {
                compact.add(prefix);
            }
        }
        return compact;
    }

    /**
     * Compares object keys by unsigned UTF-8 byte order, matching object-store listing order.
     */
    public static int compareUtf8Binary(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int commonLength = Math.min(leftBytes.length, rightBytes.length);
        for (int i = 0; i < commonLength; i++) {
            int result = Integer.compare(
                    Byte.toUnsignedInt(leftBytes[i]), Byte.toUnsignedInt(rightBytes[i]));
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    /**
     * Expands bounded {N..M} numeric ranges inside brace groups into comma-separated alternatives.
     */
    public static String expandNumericRanges(String pattern) {
        java.util.regex.Pattern simpleRange = java.util.regex.Pattern.compile(
                "\\{(\\d+)\\.\\.(\\d+)\\}");
        java.util.regex.Pattern braceGroup = java.util.regex.Pattern.compile(
                "\\{([^}]*\\d+\\.\\.\\d+[^}]*)\\}");
        java.util.regex.Matcher m = braceGroup.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String content = m.group(1);
            boolean isMixed = content.contains(",");
            if (!isMixed) {
                java.util.regex.Matcher sm = simpleRange.matcher(m.group(0));
                if (!sm.matches()) {
                    continue;
                }
            }
            String[] segments = content.split(",", -1);
            java.util.LinkedHashSet<String> values = new java.util.LinkedHashSet<>();
            boolean canExpand = true;
            for (String seg : segments) {
                java.util.regex.Matcher rm = NUMERIC_RANGE_ALTERNATIVE.matcher(seg.trim());
                if (rm.matches()) {
                    long from;
                    long to;
                    try {
                        from = Long.parseLong(rm.group(1));
                        to = Long.parseLong(rm.group(2));
                    } catch (NumberFormatException e) {
                        canExpand = false;
                        break;
                    }
                    long cardinality = numericRangeCardinality(from, to);
                    if (cardinality < 0
                            || cardinality > MAX_EXPANDED_GLOB_LIST_PREFIXES - values.size()) {
                        canExpand = false;
                        break;
                    }
                    long step = from <= to ? 1L : -1L;
                    boolean zeroPad = hasLeadingZero(rm.group(1)) || hasLeadingZero(rm.group(2));
                    int width = Math.max(digitWidth(rm.group(1)), digitWidth(rm.group(2)));
                    for (long offset = 0; offset < cardinality; offset++) {
                        long value = step > 0 ? from + offset : from - offset;
                        values.add(formatRangeValue(value, width, zeroPad));
                    }
                } else {
                    values.add(seg);
                    if (values.size() > MAX_EXPANDED_GLOB_LIST_PREFIXES) {
                        canExpand = false;
                        break;
                    }
                }
            }
            if (!canExpand) {
                continue;
            }
            StringBuilder expansion = new StringBuilder("{");
            expansion.append(String.join(",", values));
            expansion.append('}');
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(expansion.toString()));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static boolean containsNumericRange(String text) {
        return NUMERIC_RANGE_PATTERN.matcher(text).find();
    }

    private static long numericRangeCardinality(long from, long to) {
        try {
            long distance = from <= to
                    ? Math.subtractExact(to, from)
                    : Math.subtractExact(from, to);
            return Math.addExact(distance, 1L);
        } catch (ArithmeticException e) {
            return -1L;
        }
    }

    private static boolean hasLeadingZero(String value) {
        String digits = value.startsWith("-") ? value.substring(1) : value;
        return digits.length() > 1 && digits.charAt(0) == '0';
    }

    private static int digitWidth(String value) {
        return value.startsWith("-") ? value.length() - 1 : value.length();
    }

    private static String formatRangeValue(long value, int width, boolean zeroPad) {
        if (!zeroPad) {
            return Long.toString(value);
        }
        if (value == Long.MIN_VALUE) {
            return Long.toString(value);
        }
        String sign = value < 0 ? "-" : "";
        String digits = Long.toString(value < 0 ? -value : value);
        StringBuilder padded = new StringBuilder(sign);
        for (int i = digits.length(); i < width; i++) {
            padded.append('0');
        }
        return padded.append(digits).toString();
    }

    /**
     * Translates a Java NIO glob pattern to a regex matched against raw object keys.
     */
    public static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        appendGlobRegex(sb, glob);
        sb.append('$');
        return sb.toString();
    }

    private static void appendGlobRegex(StringBuilder sb, String glob) {
        boolean inClass = false;
        boolean inGroup = false;
        int i = 0;
        while (i < glob.length()) {
            char c = glob.charAt(i);
            if (c == '\\') {
                if (i + 1 < glob.length()) {
                    sb.append(Pattern.quote(String.valueOf(glob.charAt(i + 1))));
                    i += 2;
                } else {
                    sb.append("\\\\");
                    i++;
                }
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                    sb.append(']');
                } else if (c == '\\' || c == '[') {
                    sb.append('\\').append(c);
                } else {
                    sb.append(c);
                }
                i++;
                continue;
            }
            switch (c) {
                case '*':
                    if (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        sb.append(".*");
                        i += 2;
                    } else {
                        sb.append("[^/]*");
                        i++;
                    }
                    break;
                case '?':
                    sb.append("[^/]");
                    i++;
                    break;
                case '[':
                    inClass = true;
                    sb.append('[');
                    i++;
                    if (i < glob.length() && glob.charAt(i) == '!') {
                        sb.append('^');
                        i++;
                    }
                    break;
                case '{':
                    int closeIndex = findClosingBrace(glob, i);
                    if (closeIndex >= 0) {
                        appendBraceGroupRegex(sb, glob.substring(i + 1, closeIndex));
                        i = closeIndex + 1;
                        break;
                    }
                    inGroup = true;
                    sb.append("(?:");
                    i++;
                    break;
                case '}':
                    inGroup = false;
                    sb.append(')');
                    i++;
                    break;
                case ',':
                    if (inGroup) {
                        sb.append('|');
                    } else {
                        sb.append(',');
                    }
                    i++;
                    break;
                default:
                    if ("\\.^$|+()".indexOf(c) >= 0) {
                        sb.append('\\').append(c);
                    } else {
                        sb.append(c);
                    }
                    i++;
                    break;
            }
        }
    }

    private static void appendBraceGroupRegex(StringBuilder sb, String content) {
        List<String> alternatives = splitBraceAlternatives(content);
        if (alternatives.isEmpty()) {
            sb.append("(?:)");
            return;
        }
        boolean isMixed = alternatives.size() > 1;
        sb.append("(?:");
        for (int i = 0; i < alternatives.size(); i++) {
            if (i > 0) {
                sb.append('|');
            }
            String rangeRegex = numericRangeRegex(alternatives.get(i), isMixed);
            if (rangeRegex != null) {
                sb.append(rangeRegex);
            } else {
                appendGlobRegex(sb, alternatives.get(i));
            }
        }
        sb.append(')');
    }

    private static String numericRangeRegex(String alternative, boolean isMixed) {
        Matcher matcher = NUMERIC_RANGE_ALTERNATIVE.matcher(alternative.trim());
        if (!matcher.matches()) {
            return null;
        }
        String fromText = matcher.group(1);
        String toText = matcher.group(2);
        if (!isMixed && (fromText.startsWith("-") || toText.startsWith("-"))) {
            return null;
        }
        BigInteger from = new BigInteger(fromText);
        BigInteger to = new BigInteger(toText);
        BigInteger min = from.min(to);
        BigInteger max = from.max(to);
        boolean zeroPad = hasLeadingZero(fromText) || hasLeadingZero(toText);
        int width = Math.max(digitWidth(fromText), digitWidth(toText));

        List<String> alternatives = new ArrayList<>();
        if (min.signum() < 0) {
            BigInteger negativeUpper = max.min(BigInteger.ONE.negate());
            alternatives.add("-" + unsignedRangeRegex(
                    negativeUpper.abs(), min.abs(), width, zeroPad));
        }
        if (min.signum() <= 0 && max.signum() >= 0) {
            alternatives.add(zeroPad ? repeatDigit('0', width) : "0");
        }
        if (max.signum() > 0) {
            alternatives.add(unsignedRangeRegex(
                    min.max(BigInteger.ONE), max, width, zeroPad));
        }
        return joinRegexAlternatives(alternatives);
    }

    private static String unsignedRangeRegex(BigInteger from, BigInteger to,
            int width, boolean zeroPad) {
        if (zeroPad) {
            return sameLengthRangeRegex(leftPadWithZeros(from.toString(), width),
                    leftPadWithZeros(to.toString(), width));
        }
        List<String> alternatives = new ArrayList<>();
        int minLength = from.toString().length();
        int maxLength = to.toString().length();
        for (int length = minLength; length <= maxLength; length++) {
            BigInteger low = from.max(minValueForLength(length));
            BigInteger high = to.min(maxValueForLength(length));
            if (low.compareTo(high) <= 0) {
                alternatives.add(sameLengthRangeRegex(low.toString(), high.toString()));
            }
        }
        return joinRegexAlternatives(alternatives);
    }

    private static BigInteger minValueForLength(int length) {
        if (length == 1) {
            return BigInteger.ZERO;
        }
        return BigInteger.TEN.pow(length - 1);
    }

    private static BigInteger maxValueForLength(int length) {
        return BigInteger.TEN.pow(length).subtract(BigInteger.ONE);
    }

    private static String sameLengthRangeRegex(String low, String high) {
        if (low.isEmpty()) {
            return "";
        }
        if (low.equals(high)) {
            return low;
        }
        if (isAll(low, '0') && isAll(high, '9')) {
            return digitRepeat(low.length());
        }

        char lowFirst = low.charAt(0);
        char highFirst = high.charAt(0);
        String lowRest = low.substring(1);
        String highRest = high.substring(1);
        if (lowFirst == highFirst) {
            return lowFirst + sameLengthRangeRegex(lowRest, highRest);
        }
        int restLength = low.length() - 1;

        List<String> alternatives = new ArrayList<>();
        alternatives.add(lowFirst + sameLengthRangeRegex(
                lowRest, repeatDigit('9', restLength)));
        if (lowFirst + 1 <= highFirst - 1) {
            alternatives.add(digitClass((char) (lowFirst + 1), (char) (highFirst - 1))
                    + digitRepeat(restLength));
        }
        alternatives.add(highFirst + sameLengthRangeRegex(
                repeatDigit('0', restLength), highRest));
        return joinRegexAlternatives(alternatives);
    }

    private static boolean isAll(String value, char expected) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) != expected) {
                return false;
            }
        }
        return true;
    }

    private static String leftPadWithZeros(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return repeatDigit('0', width - value.length()) + value;
    }

    private static String digitClass(char from, char to) {
        if (from == to) {
            return String.valueOf(from);
        }
        return "[" + from + "-" + to + "]";
    }

    private static String digitRepeat(int count) {
        if (count == 0) {
            return "";
        }
        if (count == 1) {
            return "\\d";
        }
        return "\\d{" + count + "}";
    }

    private static String repeatDigit(char digit, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(digit);
        }
        return sb.toString();
    }

    private static String joinRegexAlternatives(List<String> alternatives) {
        if (alternatives.size() == 1) {
            return alternatives.get(0);
        }
        return "(?:" + String.join("|", alternatives) + ")";
    }

    private static class PrefixExpansion {
        private final List<String> values;
        private final int nextIndex;

        private PrefixExpansion(List<String> values, int nextIndex) {
            this.values = values;
            this.nextIndex = nextIndex;
        }
    }
}
