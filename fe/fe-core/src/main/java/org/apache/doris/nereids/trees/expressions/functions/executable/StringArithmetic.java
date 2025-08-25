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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * executable functions:
 * concat
 */
public class StringArithmetic {
    private static Literal castStringLikeLiteral(StringLikeLiteral first, String value) {
        if (first instanceof StringLiteral) {
            return new StringLiteral(value);
        } else if (first instanceof VarcharLiteral) {
            return new VarcharLiteral(value);
        }
        throw new AnalysisException("Unsupported string literal type: " + first.getClass().getSimpleName());
    }

    /**
     * Executable arithmetic functions concat
     */
    @ExecFunction(name = "concat")
    public static Expression concatVarchar(StringLikeLiteral... values) {
        final StringBuilder sb = new StringBuilder();
        for (StringLikeLiteral value : values) {
            sb.append(value.getValue());
        }
        return castStringLikeLiteral(values[0], sb.toString());
    }

    private static String substringImpl(String first, int second, int third) {
        int stringLength = first.codePointCount(0, first.length());
        if (stringLength == 0) {
            return "";
        }
        long leftIndex = 0;
        if (second < (- stringLength)) {
            return "";
        } else if (second < 0) {
            leftIndex = stringLength + second;
        } else if (second <= stringLength) {
            leftIndex = second - 1;
        } else {
            return "";
        }
        long rightIndex = 0;
        if (third <= 0) {
            return "";
        } else if ((third + leftIndex) > stringLength) {
            rightIndex = stringLength;
        } else {
            rightIndex = third + leftIndex;
        }
        // at here leftIndex and rightIndex can not be exceeding boundary
        int finalLeftIndex = first.offsetByCodePoints(0, (int) leftIndex);
        int finalRightIndex = first.offsetByCodePoints(0, (int) rightIndex);
        // left index and right index are in integer range because of definition, so we can safely cast it to int
        return first.substring(finalLeftIndex, finalRightIndex);
    }

    /**
     * Executable arithmetic functions substring
     */
    @ExecFunction(name = "substring")
    public static Expression substringVarcharIntInt(StringLikeLiteral first,
                                                        IntegerLiteral second, IntegerLiteral third) {
        return castStringLikeLiteral(first, substringImpl(first.getValue(), second.getValue(), third.getValue()));
    }

    /**
     * Executable arithmetic functions length
     */
    @ExecFunction(name = "length")
    public static Expression lengthVarchar(StringLikeLiteral first) {
        return new IntegerLiteral(first.getValue().getBytes(StandardCharsets.UTF_8).length);
    }

    /**
     * Executable arithmetic functions Lower
     */
    @ExecFunction(name = "lower")
    public static Expression lowerVarchar(StringLikeLiteral first) {
        StringBuilder result = new StringBuilder(first.getValue().length());
        for (char c : first.getValue().toCharArray()) {
            result.append(Character.toLowerCase(c));
        }
        return castStringLikeLiteral(first, result.toString());
    }

    /**
     * Executable arithmetic functions Upper
     */
    @ExecFunction(name = "upper")
    public static Expression upperVarchar(StringLikeLiteral first) {
        StringBuilder result = new StringBuilder(first.getValue().length());
        for (char c : first.getValue().toCharArray()) {
            result.append(Character.toUpperCase(c));
        }
        return castStringLikeLiteral(first, result.toString());
    }

    private static String trimImpl(String first, String second, boolean left, boolean right) {
        String result = first;
        String afterReplace = first;
        if (left) {
            do {
                result = afterReplace;
                if (result.startsWith(second)) {
                    afterReplace = result.substring(second.length());
                }
            } while (!afterReplace.equals(result));
        }
        if (right) {
            do {
                result = afterReplace;
                if (result.endsWith(second)) {
                    afterReplace = result.substring(0, result.length() - second.length());
                }
            } while (!afterReplace.equals(result));
        }
        return result;
    }

    private static String trimInImpl(String first, String second, boolean left, boolean right) {
        StringBuilder result = new StringBuilder(first);

        if (left) {
            int start = 0;
            while (start < result.length() && second.indexOf(result.charAt(start)) != -1) {
                start++;
            }
            result.delete(0, start);
        }
        if (right) {
            int end = result.length();
            while (end > 0 && second.indexOf(result.charAt(end - 1)) != -1) {
                end--;
            }
            result.delete(end, result.length());
        }

        return result.toString();
    }

    /**
     * Executable arithmetic functions Trim
     */
    @ExecFunction(name = "trim")
    public static Expression trimVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, trimImpl(first.getValue(), " ", true, true));
    }

    /**
     * Executable arithmetic functions Trim
     */
    @ExecFunction(name = "trim")
    public static Expression trimVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return castStringLikeLiteral(first, trimImpl(first.getValue(), second.getValue(), true, true));
    }

    /**
     * Executable arithmetic functions ltrim
     */
    @ExecFunction(name = "ltrim")
    public static Expression ltrimVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, trimImpl(first.getValue(), " ", true, false));
    }

    /**
     * Executable arithmetic functions ltrim
     */
    @ExecFunction(name = "ltrim")
    public static Expression ltrimVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return castStringLikeLiteral(first, trimImpl(first.getValue(), second.getValue(), true, false));
    }

    /**
     * Executable arithmetic functions rtrim
     */
    @ExecFunction(name = "rtrim")
    public static Expression rtrimVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, trimImpl(first.getValue(), " ", false, true));
    }

    /**
     * Executable arithmetic functions rtrim
     */
    @ExecFunction(name = "rtrim")
    public static Expression rtrimVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return castStringLikeLiteral(first, trimImpl(first.getValue(), second.getValue(), false, true));
    }

    /**
     * Executable arithmetic functions Trim_In
     */
    @ExecFunction(name = "trim_in")
    public static Expression trimInVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, trimInImpl(first.getValue(), " ", true, true));
    }

    /**
     * Executable arithmetic functions Trim_In
     */
    @ExecFunction(name = "trim_in")
    public static Expression trimInVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return castStringLikeLiteral(first, trimInImpl(first.getValue(), second.getValue(), true, true));
    }

    /**
     * Executable arithmetic functions ltrim_in
     */
    @ExecFunction(name = "ltrim_in")
    public static Expression ltrimInVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, trimInImpl(first.getValue(), " ", true, false));
    }

    /**
     * Executable arithmetic functions ltrim_in
     */
    @ExecFunction(name = "ltrim_in")
    public static Expression ltrimInVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return castStringLikeLiteral(first, trimInImpl(first.getValue(), second.getValue(), true, false));
    }

    /**
     * Executable arithmetic functions rtrim_in
     */
    @ExecFunction(name = "rtrim_in")
    public static Expression rtrimInVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, trimInImpl(first.getValue(), " ", false, true));
    }

    /**
     * Executable arithmetic functions rtrim_in
     */
    @ExecFunction(name = "rtrim_in")
    public static Expression rtrimInVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return castStringLikeLiteral(first, trimInImpl(first.getValue(), second.getValue(), false, true));
    }

    /**
     * Executable arithmetic functions Replace
     */
    @ExecFunction(name = "replace")
    public static Expression replace(StringLikeLiteral first, StringLikeLiteral second, StringLikeLiteral third) {
        if (second.getValue().length() == 0) {
            return castStringLikeLiteral(first, first.getValue());
        }
        return castStringLikeLiteral(first, first.getValue().replace(second.getValue(), third.getValue()));
    }

    /**
     * Executable arithmetic functions Left
     */
    @ExecFunction(name = "left")
    public static Expression left(StringLikeLiteral first, IntegerLiteral second) {
        int inputLength = first.getValue().codePointCount(0, first.getValue().length());
        if (second.getValue() <= 0) {
            return castStringLikeLiteral(first, "");
        } else if (second.getValue() >= inputLength) {
            return first;
        } else {
            // at here leftIndex and rightIndex can not be exceeding boundary
            int index = first.getValue().offsetByCodePoints(0, second.getValue());
            return castStringLikeLiteral(first, first.getValue().substring(0, index));
        }
    }

    /**
     * Executable arithmetic functions Right
     */
    @ExecFunction(name = "right")
    public static Expression right(StringLikeLiteral first, IntegerLiteral second) {
        int inputLength = first.getValue().codePointCount(0, first.getValue().length());
        if (second.getValue() < (- inputLength) || Math.abs(second.getValue()) == 0) {
            return castStringLikeLiteral(first, "");
        } else if (second.getValue() >= inputLength) {
            return first;
        } else {
            // at here second can not be exceeding boundary
            if (second.getValue() >= 0) {
                int index = first.getValue().offsetByCodePoints(0, second.getValue());
                return castStringLikeLiteral(first, first.getValue().substring(
                    inputLength - index, inputLength));
            } else {
                int index = first.getValue().offsetByCodePoints(0, Math.abs(second.getValue()) - 1);
                return castStringLikeLiteral(first, first.getValue().substring(
                    index, inputLength));
            }
        }
    }

    /**
     * Executable arithmetic functions Locate
     */
    @ExecFunction(name = "locate")
    public static Expression locate(StringLikeLiteral first, StringLikeLiteral second) {
        return locate(first, second, new IntegerLiteral(1));
    }

    /**
     * Executable arithmetic functions Locate
     */
    @ExecFunction(name = "locate")
    public static Expression locate(StringLikeLiteral first, StringLikeLiteral second, IntegerLiteral third) {
        String substr = first.getValue();
        String str = second.getValue();
        int startPos = third.getValue();

        // Handle empty substring case
        if (substr.isEmpty() && str.isEmpty() && startPos == 1) {
            return new IntegerLiteral(1);
        }

        // Check if start position is invalid
        int strLength = str.codePointCount(0, str.length());
        if (startPos <= 0 || startPos > strLength) {
            return new IntegerLiteral(0);
        }

        // Handle empty substring case
        if (substr.isEmpty()) {
            return new IntegerLiteral(startPos);
        }

        // Adjust the string based on start position (startPos is 1-indexed)
        int offset = str.offsetByCodePoints(0, startPos - 1);
        String adjustedStr = str.substring(offset);

        // Find the match position
        int matchPos = adjustedStr.indexOf(substr);
        if (matchPos >= 0) {
            // Calculate character position (not byte position)
            int charPos = adjustedStr.codePointCount(0, matchPos);
            // Return position in the original string (1-indexed)
            return new IntegerLiteral(startPos + charPos);
        } else {
            return new IntegerLiteral(0);
        }
    }

    /**
     * Executable arithmetic functions Instr
     */
    @ExecFunction(name = "instr")
    public static Expression instr(StringLikeLiteral first, StringLikeLiteral second) {
        return new IntegerLiteral(first.getValue().indexOf(second.getValue()) + 1);
    }

    /**
     * Executable arithmetic functions Ascii
     */
    @ExecFunction(name = "ascii")
    public static Expression ascii(StringLikeLiteral first) throws UnsupportedEncodingException {
        if (first.getValue().length() == 0) {
            return new IntegerLiteral(0);
        }
        String character = first.getValue();
        byte[] utf8Bytes = character.getBytes("UTF-8");
        int firstByteAscii = utf8Bytes[0] & 0xFF;
        return new IntegerLiteral(firstByteAscii);
    }

    /**
     * Executable arithmetic functions Bin
     */
    @ExecFunction(name = "bin")
    public static Expression bin(BigIntLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(first.getValue()));
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(name = "concat_ws")
    public static Expression concatWsVarcharArray(StringLikeLiteral first, ArrayLiteral second) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < second.getValue().size() - 1; i++) {
            if (!(second.getValue().get(i) instanceof NullLiteral)) {
                sb.append(second.getValue().get(i).getValue());
                sb.append(first.getValue());
            }
        }
        sb.append(second.getValue().get(second.getValue().size() - 1).getValue());
        return castStringLikeLiteral(first, sb.toString());
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(name = "concat_ws")
    public static Expression concatWsVarcharVarchar(StringLikeLiteral first, StringLikeLiteral... second) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < second.length - 1; i++) {
            sb.append(second[i].getValue());
            sb.append(first.getValue());
        }
        sb.append(second[second.length - 1].getValue());
        return castStringLikeLiteral(first, sb.toString());
    }

    /**
     * Executable arithmetic functions CharacterLength
     */
    @ExecFunction(name = "character_length")
    public static Expression characterLength(StringLikeLiteral first) {
        return new IntegerLiteral(first.getValue().codePointCount(0, first.getValue().length()));
    }

    /**
     * Executable arithmetic functions initCap
     */
    @ExecFunction(name = "initcap")
    public static Expression initCap(StringLikeLiteral first) {
        StringBuilder result = new StringBuilder(first.getValue().length());
        boolean capitalizeNext = true;

        for (char c : first.getValue().toCharArray()) {
            if (!Character.isLetterOrDigit(c)) {
                result.append(c);
                capitalizeNext = true;  // Next character should be capitalized
            } else if (capitalizeNext) {
                result.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                result.append(Character.toLowerCase(c));
            }
        }
        return castStringLikeLiteral(first, result.toString());
    }

    /**
     * Executable arithmetic functions md5
     */
    @ExecFunction(name = "md5")
    public static Expression md5(StringLikeLiteral first) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            // Update the digest with the input bytes
            md.update(first.getValue().getBytes());
            return castStringLikeLiteral(first, bytesToHex(md.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executable arithmetic functions md5
     */
    @ExecFunction(name = "md5sum")
    public static Expression md5Sum(StringLikeLiteral... first) {
        try {
            // Step 1: Create a MessageDigest instance for MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // Step 2: Concatenate all strings in the list into one string
            StringBuilder combinedInput = new StringBuilder();
            for (StringLikeLiteral input : first) {
                combinedInput.append(input.getValue());
            }

            // Step 3: Convert the combined string to a byte array and pass it to the digest() method
            byte[] messageDigest = md.digest(combinedInput.toString().getBytes());

            // Step 4: Convert the byte array into a hexadecimal string
            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0'); // Add leading zero if needed
                }
                hexString.append(hex);
            }

            // Step 5: Return the hexadecimal string
            return castStringLikeLiteral(first[0], hexString.toString());

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    // Helper method to convert a byte array to a hexadecimal string
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static int compareLiteral(Literal first, Literal... second) {
        for (int i = 0; i < second.length; i++) {
            if (second[i].getValue().equals(first.getValue())) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldInt(IntegerLiteral first, IntegerLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldTinyInt(TinyIntLiteral first, TinyIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldSmallInt(SmallIntLiteral first, SmallIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldBigInt(BigIntLiteral first, BigIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldLargeInt(LargeIntLiteral first, LargeIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldFloat(FloatLiteral first, FloatLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldDouble(DoubleLiteral first, DoubleLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldDecimalV2(DecimalLiteral first, DecimalLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldDecimalV3(DecimalV3Literal first, DecimalV3Literal... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldDateTime(DateTimeLiteral first, DateTimeLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldDateTimeV2(DateTimeV2Literal first, DateTimeV2Literal... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(name = "field")
    public static Expression fieldVarchar(StringLikeLiteral first, StringLikeLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    private static int findStringInSet(String target, String input) {
        String[] split = input.split(",", -1);
        for (int i = 0; i < split.length; i++) {
            if (split[i].equals(target)) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Executable arithmetic functions find_in_set
     */
    @ExecFunction(name = "find_in_set")
    public static Expression findInSetVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        return new IntegerLiteral(findStringInSet(first.getValue(), second.getValue()));
    }

    /**
     * Executable arithmetic functions repeat
     */
    @ExecFunction(name = "repeat")
    public static Expression repeat(StringLikeLiteral first, IntegerLiteral second) {
        // when it is too large for fe to make result string, do not folding on fe, limit 1 MB
        if ((first.getValue().length() * second.getValue()) > 1000000) {
            throw new AnalysisException("repeat too large to fold const by fe");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < second.getValue(); i++) {
            sb.append(first.getValue());
        }
        return castStringLikeLiteral(first, sb.toString());
    }

    /**
     * Executable arithmetic functions reverse
     */
    @ExecFunction(name = "reverse")
    public static Expression reverseVarchar(StringLikeLiteral first) {
        StringBuilder sb = new StringBuilder();
        sb.append(first.getValue());
        return castStringLikeLiteral(first, sb.reverse().toString());
    }

    /**
     * Executable arithmetic functions space
     */
    @ExecFunction(name = "space")
    public static Expression space(IntegerLiteral first) {
        // when it is too large for fe to make result string, do not folding on fe, limit 1 MB
        if (first.getValue() > 1000000) {
            throw new AnalysisException("space too large to fold const by fe");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < first.getValue(); i++) {
            sb.append(' ');
        }
        return new VarcharLiteral(sb.toString());
    }

    /**
     * split by char by empty string considering emoji
     * @param str input string to be split
     * @return ArrayLiteral
     */
    public static List<String> splitByGrapheme(StringLikeLiteral str) {
        List<String> result = Lists.newArrayListWithExpectedSize(str.getValue().length());
        int length = str.getValue().length();
        for (int i = 0; i < length; ) {
            int codePoint = str.getValue().codePointAt(i);
            int charCount = Character.charCount(codePoint);
            result.add(new String(new int[]{codePoint}, 0, 1));
            i += charCount;
        }
        return result;
    }

    /**
     * Executable arithmetic functions split_by_string
     */
    @ExecFunction(name = "split_by_string")
    public static Expression splitByString(StringLikeLiteral first, StringLikeLiteral second) {
        if (first.getValue().isEmpty()) {
            return new ArrayLiteral(ImmutableList.of(), ArrayType.of(first.getDataType()));
        }
        if (second.getValue().isEmpty()) {
            List<Literal> result = Lists.newArrayListWithExpectedSize(first.getValue().length());
            for (String resultStr : splitByGrapheme(first)) {
                result.add(castStringLikeLiteral(first, resultStr));
            }
            return new ArrayLiteral(result);
        }
        String[] result = first.getValue().split(Pattern.quote(second.getValue()), -1);
        List<Literal> items = new ArrayList<>();
        for (String s : result) {
            items.add(castStringLikeLiteral(first, s));
        }
        return new ArrayLiteral(items);
    }

    /**
     * Executable arithmetic functions split_part
     */
    @ExecFunction(name = "split_part")
    public static Expression splitPart(StringLikeLiteral first, StringLikeLiteral chr, IntegerLiteral number) {
        if (number.getValue() == 0) {
            return new NullLiteral(first.getDataType());
        }
        if (chr.getValue().isEmpty()) {
            return castStringLikeLiteral(first, "");
        }
        if (first.getValue().equals(chr.getValue())) {
            if (Math.abs(number.getValue()) == 1 || Math.abs(number.getValue()) == 2) {
                return castStringLikeLiteral(first, "");
            } else {
                return new NullLiteral(first.getDataType());
            }
        }
        String separator = chr.getValue();
        String[] parts;
        if (number.getValue() < 0) {
            StringBuilder sb = new StringBuilder(first.getValue());
            StringBuilder separatorBuilder = new StringBuilder(separator);
            separator = separatorBuilder.reverse().toString();
            parts = sb.reverse().toString().split(Pattern.quote(separator), -1);
        } else {
            parts = first.getValue().split(Pattern.quote(separator), -1);
        }

        if (parts.length < Math.abs(number.getValue())) {
            return new NullLiteral(first.getDataType());
        } else if (number.getValue() < 0) {
            StringBuilder result = new StringBuilder(parts[Math.abs(number.getValue()) - 1]);
            return castStringLikeLiteral(first, result.reverse().toString());
        } else {
            return castStringLikeLiteral(first, parts[number.getValue() - 1]);
        }
    }

    /**
     * Executable arithmetic functions substring_index
     */
    @ExecFunction(name = "substring_index")
    public static Expression substringIndex(StringLikeLiteral first, StringLikeLiteral chr, IntegerLiteral number) {
        if (chr.getValue().isEmpty()) {
            return chr;
        }
        String[] parts = first.getValue().split(Pattern.quote(chr.getValue()), -1);
        if (Math.abs(number.getValue()) >= parts.length) {
            return first;
        }
        int leftIndex;
        int rightIndex;
        if (parts.length < number.getValue() || number.getValue() < (- parts.length) || number.getValue() == 0) {
            return castStringLikeLiteral(first, "");
        } else if (number.getValue() < 0) {
            leftIndex = parts.length + number.getValue();
            rightIndex = parts.length;
        } else {
            leftIndex = 0;
            rightIndex = number.getValue();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = leftIndex; i < rightIndex - 1; i++) {
            sb.append(parts[i]);
            sb.append(chr.getValue());
        }
        sb.append(parts[rightIndex - 1]);
        return castStringLikeLiteral(first, sb.toString());
    }

    /**
     * Executable arithmetic functions strcmp
     */
    @ExecFunction(name = "strcmp")
    public static Expression strcmp(StringLikeLiteral first, StringLikeLiteral second) {
        int result = first.getValue().compareTo(second.getValue());
        if (result == 0) {
            return new TinyIntLiteral((byte) 0);
        } else if (result < 0) {
            return new TinyIntLiteral((byte) -1);
        } else {
            return new TinyIntLiteral((byte) 1);
        }
    }

    /**
     * Executable arithmetic functions overlay
     */
    @ExecFunction(name = "overlay")
    public static Expression overlay(StringLikeLiteral originStr,
            IntegerLiteral pos, IntegerLiteral len, StringLikeLiteral insertStr) {
        StringBuilder sb = new StringBuilder();
        int totalLength = originStr.getValue().codePointCount(0, originStr.getValue().length());
        if (pos.getValue() <= 0 || pos.getValue() > totalLength) {
            return originStr;
        } else {
            if (len.getValue() < 0 || len.getValue() > (totalLength - pos.getValue())) {
                sb.append(substringImpl(originStr.getValue(), 1, pos.getValue() - 1));
                sb.append(insertStr.getValue());
                return castStringLikeLiteral(originStr, sb.toString());
            } else {
                sb.append(substringImpl(originStr.getValue(), 1, pos.getValue() - 1));
                sb.append(insertStr.getValue());
                sb.append(substringImpl(originStr.getValue(), pos.getValue() + len.getValue(), totalLength));
                return castStringLikeLiteral(originStr, sb.toString());
            }
        }
    }

    /**
     * Executable arithmetic functions parseurl
     */
    @ExecFunction(name = "parse_url")
    public static Expression parseurl(StringLikeLiteral first, StringLikeLiteral second) {
        URI uri = null;
        try {
            uri = new URI(first.getValue());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sb = new StringBuilder();
        switch (second.getValue().toUpperCase()) {
            case "PROTOCOL":
                String scheme = uri.getScheme();
                if (scheme == null) {
                    return new NullLiteral(first.getDataType());
                }
                sb.append(scheme); // e.g., http, https
                break;
            case "HOST":
                String host = uri.getHost();
                if (host == null) {
                    return new NullLiteral(first.getDataType());
                }
                sb.append(host);  // e.g., www.example.com
                break;
            case "PATH":
                String path = uri.getPath();
                if (path == null) {
                    return new NullLiteral(first.getDataType());
                }
                sb.append(path);  // e.g., /page
                break;
            case "REF":
                try {
                    String ref = uri.toURL().getRef();
                    if (ref == null) {
                        return new NullLiteral(first.getDataType());
                    }
                    sb.append(ref);  // e.g., /page
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                break;
            case "AUTHORITY":
                String authority = uri.getAuthority();
                if (authority == null) {
                    return new NullLiteral(first.getDataType());
                }
                sb.append(authority);  // e.g., param1=value1&param2=value2
                break;
            case "FILE":
                try {
                    String file = uri.toURL().getFile();
                    if (file == null) {
                        return new NullLiteral(first.getDataType());
                    }
                    sb.append(file);  // e.g., param1=value1&param2=value2
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                break;
            case "QUERY":
                String query = uri.getQuery();
                if (query == null) {
                    return new NullLiteral(first.getDataType());
                }
                sb.append(query);  // e.g., param1=value1&param2=value2
                break;
            case "PORT":
                sb.append(uri.getPort());
                break;
            case "USERINFO":
                String userInfo = uri.getUserInfo();
                if (userInfo == null) {
                    return new NullLiteral(first.getDataType());
                }
                sb.append(userInfo);  // e.g., user:pass
                break;
            default:
                throw new RuntimeException("Valid URL parts are 'PROTOCOL', 'HOST', "
                        + "'PATH', 'REF', 'AUTHORITY', 'FILE', 'USERINFO', 'PORT' and 'QUERY'");
        }
        return castStringLikeLiteral(first, sb.toString());
    }

    /**
     * Executable arithmetic functions urldecode
     */
    @ExecFunction(name = "url_decode")
    public static Expression urlDecode(StringLikeLiteral first) {
        try {
            return castStringLikeLiteral(first, URLDecoder.decode(first.getValue(), StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executable arithmetic functions urlencode
     */
    @ExecFunction(name = "url_encode")
    public static Expression urlEncode(StringLikeLiteral first) {
        try {
            return castStringLikeLiteral(first, URLEncoder.encode(first.getValue(), StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executable arithmetic functions append_trailing_char_if_absent
     */
    @ExecFunction(name = "append_trailing_char_if_absent")
    public static Expression appendTrailingCharIfAbsent(StringLikeLiteral first, StringLikeLiteral second) {
        if (second.getValue().codePointCount(0, second.getValue().length()) != 1) {
            return new NullLiteral(first.getDataType());
        }
        if (first.getValue().endsWith(second.getValue())) {
            return first;
        } else {
            return castStringLikeLiteral(first, first.getValue() + second.getValue());
        }
    }

    /**
     * Executable arithmetic functions endsWith
     */
    @ExecFunction(name = "ends_with")
    public static Expression endsWith(StringLikeLiteral first, StringLikeLiteral second) {
        if (first.getValue().endsWith(second.getValue())) {
            return BooleanLiteral.TRUE;
        } else {
            return BooleanLiteral.FALSE;
        }
    }

    /**
     * Executable arithmetic functions extractUrlParameter
     */
    @ExecFunction(name = "extract_url_parameter")
    public static Expression extractUrlParameter(StringLikeLiteral first, StringLikeLiteral second) {
        if (first.getValue() == null || first.getValue().indexOf('?') == -1) {
            return castStringLikeLiteral(first, "");
        }
        URI uri;
        try {
            uri = new URI(first.getValue());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        String query = uri.getQuery();
        if (query != null) {
            String[] pairs = query.split("&", -1);

            for (String pair : pairs) {
                String[] keyValue = pair.split("=", -1);
                if (second.getValue().equals(keyValue[0])) {
                    return castStringLikeLiteral(first, keyValue[1]);
                }
            }
        }
        return castStringLikeLiteral(first, "");
    }

    /**
     * Executable arithmetic functions quote
     */
    @ExecFunction(name = "quote")
    public static Expression quote(StringLikeLiteral first) {
        return castStringLikeLiteral(first, "\'" + first.getValue() + "\'");
    }

    /**
     * Executable arithmetic functions replaceEmpty
     */
    @ExecFunction(name = "replace_empty")
    public static Expression replaceEmpty(StringLikeLiteral first, StringLikeLiteral second, StringLikeLiteral third) {
        if (second.getValue().isEmpty()) {
            if (first.getValue().isEmpty()) {
                return castStringLikeLiteral(first, third.getValue());
            }
            List<String> inputs = splitByGrapheme(first);
            StringBuilder sb = new StringBuilder();
            sb.append(third.getValue());
            for (String input : inputs) {
                sb.append(input);
                sb.append(third.getValue());
            }
            return castStringLikeLiteral(first, sb.toString());
        }
        return castStringLikeLiteral(first, first.getValue().replace(second.getValue(), third.getValue()));
    }
}
