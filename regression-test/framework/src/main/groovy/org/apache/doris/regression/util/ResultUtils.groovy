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

package org.apache.doris.regression.util

import groovy.transform.CompileStatic
import org.junit.Assert

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.Blob
import java.sql.Clob
import java.sql.Struct
import java.sql.Time
import java.sql.Timestamp
import java.time.temporal.TemporalAccessor
import java.util.regex.Pattern

@CompileStatic
class ResultUtils {
    private static final Object PARSE_FAILED = new Object()
    private static final double NUMERIC_RELATIVE_TOLERANCE = 1.0E-8D
    private static final Pattern NUMBER_PATTERN = Pattern.compile(
            "[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?")
    private static final Pattern PREFIXED_HEX_PATTERN = Pattern.compile("(?i)0x[0-9a-f]*")
    private static final Pattern BARE_HEX_PATTERN = Pattern.compile("(?i)[0-9a-f]*")
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}")
    private static final Pattern TIME_PATTERN = Pattern.compile(
            "(\\d{2}:\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?")
    private static final Pattern DATETIME_PATTERN = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2})[T ](\\d{2}:\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?")
    private static final Pattern UNICODE_ESCAPE_PATTERN = Pattern.compile("[0-9a-fA-F]{4}")

    static List<List<Object>> normalizeRows(List<List<Object>> rows) {
        List<List<Object>> normalizedRows = new ArrayList<>()
        for (List<Object> row : rows) {
            List<Object> normalizedRow = new ArrayList<>()
            for (Object value : row) {
                normalizedRow.add(normalizeValue(value))
            }
            normalizedRows.add(normalizedRow)
        }
        return normalizedRows
    }

    static void assertSparkDorisResultEquals(List<List<Object>> sparkRows, List<List<Object>> dorisRows) {
        List<List<Object>> normalizedSparkRows = normalizeRows(sparkRows)
        List<List<Object>> normalizedDorisRows = normalizeRows(dorisRows)
        Assert.assertEquals("Spark and Doris result row count mismatch",
                normalizedSparkRows.size(), normalizedDorisRows.size())

        for (int rowIndex = 0; rowIndex < normalizedSparkRows.size(); rowIndex++) {
            List<Object> sparkRow = normalizedSparkRows.get(rowIndex)
            List<Object> dorisRow = normalizedDorisRows.get(rowIndex)
            Assert.assertEquals("Spark and Doris result column count mismatch at row " + (rowIndex + 1),
                    sparkRow.size(), dorisRow.size())
            for (int columnIndex = 0; columnIndex < sparkRow.size(); columnIndex++) {
                Object sparkValue = sparkRow.get(columnIndex)
                Object dorisValue = dorisRow.get(columnIndex)
                if (!valueEquals(sparkValue, dorisValue)) {
                    Assert.fail("Spark/Doris result mismatch at row " + (rowIndex + 1)
                            + ", column " + (columnIndex + 1)
                            + "\nSpark raw      : " + valueToString(sparkRows.get(rowIndex).get(columnIndex))
                            + "\nDoris raw      : " + valueToString(dorisRows.get(rowIndex).get(columnIndex))
                            + "\nSpark normalized: " + valueToString(sparkValue)
                            + "\nDoris normalized: " + valueToString(dorisValue))
                }
            }
        }
    }

    static Object normalizeValue(Object value) {
        if (value == null) {
            return null
        }
        if (value instanceof byte[]) {
            return bytesToHex((byte[]) value)
        }
        if (value instanceof ByteBuffer) {
            return bytesToHex(byteBufferToBytes((ByteBuffer) value))
        }
        if (value instanceof BigDecimal) {
            return normalizeDecimal((BigDecimal) value)
        }
        if (value instanceof BigInteger
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return normalizeDecimal(new BigDecimal(value.toString()))
        }
        if (value instanceof Float || value instanceof Double) {
            Double doubleValue = ((Number) value).doubleValue()
            if (doubleValue.isNaN() || doubleValue.isInfinite()) {
                return doubleValue.toString().toLowerCase(Locale.ROOT)
            }
            return new ApproximateNumber(normalizeDecimal(new BigDecimal(value.toString())))
        }
        if (value instanceof Boolean) {
            return value
        }
        if (value instanceof java.sql.Array) {
            try {
                return normalizeValue(((java.sql.Array) value).getArray())
            } catch (Exception ignored) {
                return value.toString()
            }
        }
        if (value instanceof Struct) {
            try {
                return normalizeValue(Arrays.asList(((Struct) value).getAttributes()))
            } catch (Exception ignored) {
                return value.toString()
            }
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value
            try {
                return bytesToHex(blob.getBytes(1L, (int) blob.length()))
            } catch (Exception ignored) {
                return value.toString()
            }
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value
            try {
                return normalizeValue(clob.getSubString(1L, (int) clob.length()))
            } catch (Exception ignored) {
                return value.toString()
            }
        }
        if (value instanceof java.sql.Date
                || value instanceof Time
                || value instanceof Timestamp
                || value instanceof java.util.Date
                || value instanceof Calendar
                || value instanceof TemporalAccessor) {
            return normalizeString(value.toString())
        }
        if (value instanceof Map) {
            TreeMap<String, Object> normalizedMap = new TreeMap<>()
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                normalizedMap.put(canonicalKey(entry.getKey()), normalizeValue(entry.getValue()))
            }
            return normalizedMap
        }
        if (value instanceof Collection) {
            List<Object> normalizedList = new ArrayList<>()
            for (Object item : (Collection<?>) value) {
                normalizedList.add(normalizeValue(item))
            }
            return normalizedList
        }
        if (value.getClass().isArray()) {
            List<Object> normalizedList = new ArrayList<>()
            int length = java.lang.reflect.Array.getLength(value)
            for (int i = 0; i < length; i++) {
                normalizedList.add(normalizeValue(java.lang.reflect.Array.get(value, i)))
            }
            return normalizedList
        }
        if (value instanceof CharSequence) {
            return normalizeString(value.toString())
        }
        return normalizeString(value.toString())
    }

    private static Object normalizeString(String raw) {
        String text = raw.trim()
        Object parsed = parseComplexValue(text)
        if (parsed != PARSE_FAILED) {
            return normalizeValue(parsed)
        }

        parsed = parseQuotedStringValue(text)
        if (parsed != PARSE_FAILED) {
            return normalizeValue(parsed)
        }

        if (PREFIXED_HEX_PATTERN.matcher(text).matches()) {
            return normalizePrefixedHex(text)
        }

        String normalizedTemporal = normalizeTemporal(text)
        if (normalizedTemporal != null) {
            return normalizedTemporal
        }

        String lowerText = text.toLowerCase(Locale.ROOT)
        if (lowerText == "nan"
                || lowerText == "-nan"
                || lowerText == "inf"
                || lowerText == "+inf"
                || lowerText == "-inf"
                || lowerText == "infinity"
                || lowerText == "+infinity"
                || lowerText == "-infinity") {
            return lowerText
        }
        return raw
    }

    private static Object parseQuotedStringValue(String text) {
        if (text.length() < 2 || text.charAt(0) != ('"' as char)
                || text.charAt(text.length() - 1) != ('"' as char)) {
            return PARSE_FAILED
        }
        try {
            return new ComplexValueParser(text).parse()
        } catch (RuntimeException ignored) {
            return PARSE_FAILED
        }
    }

    private static Object parseComplexValue(String text) {
        if (text.length() < 2) {
            return PARSE_FAILED
        }
        char first = text.charAt(0)
        char last = text.charAt(text.length() - 1)
        if (!((first == '[' as char && last == ']' as char)
                || (first == '{' as char && last == '}' as char))) {
            return PARSE_FAILED
        }
        try {
            return new ComplexValueParser(text).parse()
        } catch (RuntimeException ignored) {
            return PARSE_FAILED
        }
    }

    private static String normalizeTemporal(String text) {
        if (DATE_PATTERN.matcher(text).matches()) {
            return text
        }

        def datetimeMatcher = DATETIME_PATTERN.matcher(text)
        if (datetimeMatcher.matches()) {
            return datetimeMatcher.group(1) + " " + normalizeTime(
                    datetimeMatcher.group(2), datetimeMatcher.group(3), datetimeMatcher.group(4))
        }

        def timeMatcher = TIME_PATTERN.matcher(text)
        if (timeMatcher.matches()) {
            return normalizeTime(timeMatcher.group(1), timeMatcher.group(2), timeMatcher.group(3))
        }

        return null
    }

    private static String normalizeTime(String hourMinute, String secondPart, String fractionPart) {
        String second = secondPart == null ? "00" : secondPart
        String fraction = fractionPart
        if (fraction != null) {
            fraction = fraction.replaceAll("0+\$", "")
        }
        String suffix = fraction == null || fraction.isEmpty() ? "" : "." + fraction
        return hourMinute + ":" + second + suffix
    }

    private static String normalizePrefixedHex(String text) {
        return "0x" + text.substring(2).toUpperCase(Locale.ROOT)
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(2 + bytes.length * 2)
        builder.append("0x")
        for (byte b : bytes) {
            builder.append(String.format("%02X", b & 0xFF))
        }
        return builder.toString()
    }

    private static byte[] byteBufferToBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer()
        byte[] bytes = new byte[copy.remaining()]
        copy.get(bytes)
        return bytes
    }

    private static BigDecimal normalizeDecimal(BigDecimal decimal) {
        BigDecimal normalized = decimal.stripTrailingZeros()
        if (normalized.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO
        }
        return normalized
    }

    private static boolean valueEquals(Object left, Object right) {
        if (left == null || right == null) {
            return left == right
        }
        if (left.equals(right)) {
            return true
        }
        if (left instanceof Map && right instanceof Map) {
            return mapEquals((Map<?, ?>) left, (Map<?, ?>) right)
        }
        if (left instanceof List && right instanceof List) {
            List<?> leftList = (List<?>) left
            List<?> rightList = (List<?>) right
            if (leftList.size() != rightList.size()) {
                return false
            }
            for (int i = 0; i < leftList.size(); i++) {
                if (!valueEquals(leftList.get(i), rightList.get(i))) {
                    return false
                }
            }
            return true
        }

        if (hexEquals(left, right)) {
            return true
        }

        if (left instanceof Boolean || right instanceof Boolean) {
            Boolean leftBoolean = asBoolean(left)
            Boolean rightBoolean = asBoolean(right)
            if (leftBoolean != null && rightBoolean != null) {
                return leftBoolean == rightBoolean
            }
        }

        BigDecimal leftDecimal = asDecimal(left)
        BigDecimal rightDecimal = asDecimal(right)
        if (leftDecimal != null && rightDecimal != null) {
            return numericEquals(left, right, leftDecimal, rightDecimal)
        }
        return left.equals(right)
    }

    private static boolean mapEquals(Map<?, ?> leftMap, Map<?, ?> rightMap) {
        if (leftMap.size() != rightMap.size()) {
            return false
        }
        if (leftMap.keySet().equals(rightMap.keySet())) {
            for (Object key : leftMap.keySet()) {
                if (!valueEquals(leftMap.get(key), rightMap.get(key))) {
                    return false
                }
            }
            return true
        }

        Set<Object> matchedRightKeys = new HashSet<>()
        for (Object leftKey : leftMap.keySet()) {
            Object matchedRightKey = findMatchedMapKey(leftKey, leftMap.get(leftKey), rightMap, matchedRightKeys)
            if (matchedRightKey == null) {
                return false
            }
            matchedRightKeys.add(matchedRightKey)
        }
        return true
    }

    private static Object findMatchedMapKey(Object leftKey, Object leftValue, Map<?, ?> rightMap,
            Set<Object> matchedRightKeys) {
        for (Object rightKey : rightMap.keySet()) {
            if (matchedRightKeys.contains(rightKey)
                    || !mapKeyEquals(leftKey, rightKey)
                    || !valueEquals(leftValue, rightMap.get(rightKey))) {
                continue
            }
            return rightKey
        }
        return null
    }

    private static boolean mapKeyEquals(Object leftKey, Object rightKey) {
        if (leftKey == null || rightKey == null) {
            return leftKey == rightKey
        }
        if (leftKey.equals(rightKey)) {
            return true
        }
        Boolean leftBoolean = booleanEquivalentMapKey(leftKey)
        Boolean rightBoolean = booleanEquivalentMapKey(rightKey)
        return leftBoolean != null && rightBoolean != null && leftBoolean == rightBoolean
    }

    private static Boolean booleanEquivalentMapKey(Object key) {
        if (key instanceof CharSequence) {
            String text = key.toString()
            if (text == "bool:true") {
                return true
            }
            if (text == "bool:false") {
                return false
            }
            if (text.startsWith("num:")) {
                return booleanFromDecimalText(text.substring("num:".length()))
            }
            return null
        }
        return asBoolean(key)
    }

    private static Boolean booleanFromDecimalText(String text) {
        try {
            BigDecimal decimal = normalizeDecimal(new BigDecimal(text))
            if (decimal.compareTo(BigDecimal.ZERO) == 0) {
                return false
            }
            if (decimal.compareTo(BigDecimal.ONE) == 0) {
                return true
            }
            return null
        } catch (NumberFormatException ignored) {
            return null
        }
    }

    private static boolean hexEquals(Object left, Object right) {
        String leftHex = asHex(left, right)
        String rightHex = asHex(right, left)
        return leftHex != null && rightHex != null && leftHex == rightHex
    }

    private static String asHex(Object value, Object counterpart) {
        if (!(value instanceof CharSequence) && !(value instanceof BigDecimal)) {
            return null
        }
        String text = value instanceof BigDecimal ? ((BigDecimal) value).toPlainString() : value.toString().trim()
        if (PREFIXED_HEX_PATTERN.matcher(text).matches()) {
            return text.substring(2).toUpperCase(Locale.ROOT)
        }
        if (!(counterpart instanceof CharSequence)
                || !PREFIXED_HEX_PATTERN.matcher(counterpart.toString().trim()).matches()
                || text.length() % 2 != 0
                || !BARE_HEX_PATTERN.matcher(text).matches()) {
            return null
        }
        return text.toUpperCase(Locale.ROOT)
    }

    private static Boolean asBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value
        }
        BigDecimal decimal = asDecimal(value)
        if (decimal != null) {
            if (decimal.compareTo(BigDecimal.ZERO) == 0) {
                return false
            }
            if (decimal.compareTo(BigDecimal.ONE) == 0) {
                return true
            }
            return null
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim().toLowerCase(Locale.ROOT)
            if (text == "true") {
                return true
            }
            if (text == "false") {
                return false
            }
        }
        return null
    }

    private static BigDecimal asDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value
        }
        if (value instanceof BigInteger
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return normalizeDecimal(new BigDecimal(value.toString()))
        }
        if (value instanceof Float || value instanceof Double) {
            Double doubleValue = ((Number) value).doubleValue()
            if (doubleValue.isNaN() || doubleValue.isInfinite()) {
                return null
            }
            return normalizeDecimal(new BigDecimal(value.toString()))
        }
        if (value instanceof ApproximateNumber) {
            return ((ApproximateNumber) value).decimal
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim()
            if (NUMBER_PATTERN.matcher(text).matches()) {
                return normalizeDecimal(new BigDecimal(text))
            }
        }
        return null
    }

    private static boolean numericEquals(Object left, Object right, BigDecimal leftDecimal, BigDecimal rightDecimal) {
        if (left instanceof CharSequence && right instanceof CharSequence) {
            return false
        }
        if (left instanceof ApproximateNumber || right instanceof ApproximateNumber
                || left instanceof Float || left instanceof Double
                || right instanceof Float || right instanceof Double) {
            return approximateDecimalEquals(leftDecimal, rightDecimal)
        }
        return leftDecimal.compareTo(rightDecimal) == 0
    }

    private static boolean approximateDecimalEquals(BigDecimal left, BigDecimal right) {
        if (left.compareTo(right) == 0) {
            return true
        }
        double leftDouble = left.doubleValue()
        double rightDouble = right.doubleValue()
        if (Double.isInfinite(leftDouble) || Double.isInfinite(rightDouble)
                || Double.isNaN(leftDouble) || Double.isNaN(rightDouble)) {
            return false
        }
        double diff = Math.abs(leftDouble - rightDouble)
        double base = Math.max(1.0D, Math.max(Math.abs(leftDouble), Math.abs(rightDouble)))
        return diff / base <= NUMERIC_RELATIVE_TOLERANCE
    }

    private static String canonicalKey(Object key) {
        Object normalizedKey = normalizeValue(key)
        if (normalizedKey instanceof Boolean) {
            return ((Boolean) normalizedKey) ? "bool:true" : "bool:false"
        }
        if (!(normalizedKey instanceof CharSequence)) {
            BigDecimal decimal = asDecimal(normalizedKey)
            if (decimal != null) {
                return "num:" + decimal.toPlainString()
            }
        }
        if (normalizedKey instanceof Map || normalizedKey instanceof List) {
            return "complex:" + valueToString(normalizedKey)
        }
        return "str:" + normalizedKey.toString()
    }

    private static String valueToString(Object value) {
        if (value == null) {
            return "null"
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString()
        }
        if (value instanceof ApproximateNumber) {
            return ((ApproximateNumber) value).decimal.toPlainString()
        }
        return value.toString()
    }

    private static final class ApproximateNumber {
        private final BigDecimal decimal

        private ApproximateNumber(BigDecimal decimal) {
            this.decimal = decimal
        }

        @Override
        String toString() {
            return decimal.toPlainString()
        }
    }

    private static final class ComplexValueParser {
        private final String text
        private int position = 0

        ComplexValueParser(String text) {
            this.text = text
        }

        Object parse() {
            Object value = parseValue(false)
            skipWhitespace()
            if (position != text.length()) {
                throw new IllegalArgumentException("Unexpected trailing content")
            }
            return value
        }

        private Object parseValue(boolean stopAtColon) {
            skipWhitespace()
            if (position >= text.length()) {
                throw new IllegalArgumentException("Unexpected end of input")
            }
            char ch = text.charAt(position)
            if (ch == '[' as char) {
                return parseArray()
            }
            if (ch == '{' as char) {
                return parseObject()
            }
            if (ch == '"' as char) {
                return parseQuotedString()
            }
            return parseToken(stopAtColon)
        }

        private List<Object> parseArray() {
            position++
            List<Object> values = new ArrayList<>()
            skipWhitespace()
            if (consume(']' as char)) {
                return values
            }
            while (true) {
                values.add(parseValue(false))
                skipWhitespace()
                if (consume(',' as char)) {
                    continue
                }
                if (consume(']' as char)) {
                    return values
                }
                throw new IllegalArgumentException("Expected ',' or ']'")
            }
        }

        private Map<Object, Object> parseObject() {
            position++
            Map<Object, Object> values = new LinkedHashMap<>()
            skipWhitespace()
            if (consume('}' as char)) {
                return values
            }
            while (true) {
                Object key = parseValue(true)
                skipWhitespace()
                if (!consume(':' as char)) {
                    throw new IllegalArgumentException("Expected ':'")
                }
                Object value = parseValue(false)
                values.put(key, value)
                skipWhitespace()
                if (consume(',' as char)) {
                    continue
                }
                if (consume('}' as char)) {
                    return values
                }
                throw new IllegalArgumentException("Expected ',' or '}'")
            }
        }

        private String parseQuotedString() {
            position++
            StringBuilder builder = new StringBuilder()
            while (position < text.length()) {
                char ch = text.charAt(position++)
                if (ch == '"' as char) {
                    return builder.toString()
                }
                if (ch == '\\' as char && position < text.length()) {
                    char escaped = text.charAt(position++)
                    switch (escaped) {
                        case '"' as char:
                        case '\\' as char:
                        case '/' as char:
                            builder.append(escaped)
                            break
                        case 'b' as char:
                            builder.append('\b' as char)
                            break
                        case 'f' as char:
                            builder.append('\f' as char)
                            break
                        case 'n' as char:
                            builder.append('\n' as char)
                            break
                        case 'r' as char:
                            builder.append('\r' as char)
                            break
                        case 't' as char:
                            builder.append('\t' as char)
                            break
                        case 'u' as char:
                            if (position + 4 <= text.length()) {
                                String hex = text.substring(position, position + 4)
                                if (UNICODE_ESCAPE_PATTERN.matcher(hex).matches()) {
                                    builder.append((char) Integer.parseInt(hex, 16))
                                    position += 4
                                    break
                                }
                            }
                            builder.append(escaped)
                            break
                        default:
                            builder.append(escaped)
                            break
                    }
                } else {
                    builder.append(ch)
                }
            }
            throw new IllegalArgumentException("Unclosed quoted string")
        }

        private Object parseToken(boolean stopAtColon) {
            int start = position
            while (position < text.length()) {
                char ch = text.charAt(position)
                if (ch == ',' as char || ch == ']' as char || ch == '}' as char
                        || (stopAtColon && ch == ':' as char)) {
                    break
                }
                position++
            }
            String token = text.substring(start, position).trim()
            if (token.isEmpty()) {
                return ""
            }
            String lowerToken = token.toLowerCase(Locale.ROOT)
            if (lowerToken == "null") {
                return null
            }
            if (lowerToken == "true") {
                return true
            }
            if (lowerToken == "false") {
                return false
            }
            if (NUMBER_PATTERN.matcher(token).matches()) {
                return normalizeDecimal(new BigDecimal(token))
            }
            return token
        }

        private void skipWhitespace() {
            while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
                position++
            }
        }

        private boolean consume(char expected) {
            if (position < text.length() && text.charAt(position) == expected) {
                position++
                return true
            }
            return false
        }
    }
}
