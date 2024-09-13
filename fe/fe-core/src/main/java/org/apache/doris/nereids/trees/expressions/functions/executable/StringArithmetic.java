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

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * executable functions:
 * concat
 */
public class StringArithmetic {
    private static Expression castStringLikeLiteral(StringLikeLiteral first, String value) {
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
    public static Expression concatVarcharVarchar(StringLikeLiteral first, StringLikeLiteral second) {
        String result = first.getValue() + second.getValue();
        return castStringLikeLiteral(first, result);
    }

    private static String substringImpl(String first, int second, int third) {
        int stringLength = first.length();
        if (stringLength == 0) {
            return "";
        }
        int leftIndex = 0;
        if (second < (- stringLength)) {
            return "";
        } else if (second < 0) {
            leftIndex = stringLength + second;
        } else if (second <= stringLength) {
            leftIndex = second - 1;
        } else {
            return "";
        }
        int rightIndex = 0;
        if (third <= 0) {
            return "";
        } else if ((third + leftIndex) > stringLength) {
            rightIndex = stringLength;
        } else {
            rightIndex = third + leftIndex;
        }
        return first.substring(leftIndex, rightIndex);
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
        return new IntegerLiteral(first.getValue().length());
    }

    /**
     * Executable arithmetic functions Lower
     */
    @ExecFunction(name = "lower")
    public static Expression lowerVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, first.getValue().toLowerCase());
    }

    /**
     * Executable arithmetic functions Upper
     */
    @ExecFunction(name = "upper")
    public static Expression upperVarchar(StringLikeLiteral first) {
        return castStringLikeLiteral(first, first.getValue().toUpperCase());
    }

    private static String trimImpl(String first, String second, boolean left, boolean right) {
        String result = first;
        String afterReplace = first;
        if (left) {
            do {
                result = afterReplace;
                afterReplace = result.replaceFirst("^" + second, "");
            } while (!afterReplace.equals(result));
        }
        if (right) {
            do {
                result = afterReplace;
                afterReplace = result.replaceFirst(second + "$", "");
            } while (!afterReplace.equals(result));
        }
        return result;
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
        if (second.getValue() <= 0) {
            return castStringLikeLiteral(first, "");
        } else if (second.getValue() < first.getValue().length()) {
            return castStringLikeLiteral(first, first.getValue().substring(0, second.getValue()));
        } else {
            return first;
        }
    }

    /**
     * Executable arithmetic functions Right
     */
    @ExecFunction(name = "right")
    public static Expression right(StringLikeLiteral first, IntegerLiteral second) {
        if (second.getValue() < (- first.getValue().length()) || Math.abs(second.getValue()) == 0) {
            return castStringLikeLiteral(first, "");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            if (second.getValue() > 0) {
                return castStringLikeLiteral(first, first.getValue().substring(
                    first.getValue().length() - second.getValue(), first.getValue().length()));
            } else {
                return castStringLikeLiteral(first, first.getValue().substring(
                    Math.abs(second.getValue()) - 1, first.getValue().length()));
            }
        }
    }

    /**
     * Executable arithmetic functions Locate
     */
    @ExecFunction(name = "locate")
    public static Expression locate(StringLikeLiteral first, StringLikeLiteral second) {
        return new IntegerLiteral(second.getValue().trim().indexOf(first.getValue()) + 1);
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
    public static Expression ascii(StringLikeLiteral first) {
        if (first.getValue().length() == 0) {
            return new IntegerLiteral(0);
        }
        char firstChar = first.getValue().charAt(0);
        return new IntegerLiteral(firstChar);
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
    public static Expression concatWsVarcharVarchar(StringLikeLiteral first, VarcharLiteral... second) {
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
        return new IntegerLiteral(first.getValue().length());
    }

    private static boolean isSeparator(char c) {
        if (".$|()[{^?*+\\".indexOf(c) == -1) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Executable arithmetic functions initCap
     */
    @ExecFunction(name = "initcap")
    public static Expression initCap(StringLikeLiteral first) {
        StringBuilder result = new StringBuilder(first.getValue().length());
        boolean capitalizeNext = true;

        for (char c : first.getValue().toCharArray()) {
            if (Character.isWhitespace(c) || isSeparator(c)) {
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
    public static Expression md5Sum(VarcharLiteral... first) {
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
    public static Expression fieldVarchar(StringLikeLiteral first, VarcharLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    private static int findStringInSet(String target, String input) {
        String[] split = input.split(",");
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
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < first.getValue(); i++) {
            sb.append(' ');
        }
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions split_by_char
     */
    @ExecFunction(name = "split_by_char")
    public static Expression splitByChar(StringLikeLiteral first, StringLikeLiteral second) {
        String[] result = first.getValue().split(second.getValue());
        List<Literal> items = new ArrayList<>();
        for (int i = 1; i < result.length; i++) {
            items.add((Literal) castStringLikeLiteral(first, result[i]));
        }
        return new ArrayLiteral(items);
    }

    /**
     * Executable arithmetic functions split_part
     */
    @ExecFunction(name = "split_part")
    public static Expression splitPart(StringLikeLiteral first, StringLikeLiteral chr, IntegerLiteral number) {
        if (first.getValue().equals(chr.getValue())) {
            if (Math.abs(number.getValue()) == 1 || Math.abs(number.getValue()) == 2) {
                return castStringLikeLiteral(first, "");
            }
        }
        String separator = chr.getValue();
        String[] parts = null;
        if (number.getValue() < 0) {
            StringBuilder sb = new StringBuilder(first.getValue());
            StringBuilder seperatorBuilder = new StringBuilder(separator);
            separator = seperatorBuilder.reverse().toString();
            if (".$|()[{^?*+\\".contains(separator) || separator.startsWith("\\")) {
                separator = "\\" + separator;
            }
            parts = sb.reverse().toString().split(separator);
        } else {
            if (".$|()[{^?*+\\".contains(separator) || separator.startsWith("\\")) {
                separator = "\\" + separator;
            }
            parts = first.getValue().split(separator);
        }

        if (parts.length < Math.abs(number.getValue()) || number.getValue() == 0) {
            if (parts.length == Math.abs(number.getValue()) - 1) {
                if (number.getValue() < 0 && first.getValue().startsWith(chr.getValue())
                        || number.getValue() > 0 && first.getValue().endsWith(chr.getValue())) {
                    return castStringLikeLiteral(first, "");
                }
            }
            return new NullLiteral();
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
        String[] parts = first.getValue().split(chr.getValue());
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
     * Executable arithmetic functions strLeft
     */
    @ExecFunction(name = "strleft")
    public static Expression strLeft(StringLikeLiteral first, IntegerLiteral second) {
        if (second.getValue() <= 0) {
            return castStringLikeLiteral(first, "");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            return castStringLikeLiteral(first, first.getValue().substring(0, second.getValue()));
        }
    }

    /**
     * Executable arithmetic functions strRight
     */
    @ExecFunction(name = "strright")
    public static Expression strRight(StringLikeLiteral first, IntegerLiteral second) {
        if (second.getValue() < (- first.getValue().length()) || Math.abs(second.getValue()) == 0) {
            return castStringLikeLiteral(first, "");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            if (second.getValue() > 0) {
                return castStringLikeLiteral(first, first.getValue().substring(
                    first.getValue().length() - second.getValue(), first.getValue().length()));
            } else {
                return castStringLikeLiteral(first, first.getValue().substring(
                    Math.abs(second.getValue()) - 1, first.getValue().length()));
            }
        }
    }

    /**
     * Executable arithmetic functions overlay
     */
    @ExecFunction(name = "overlay")
    public static Expression overlay(StringLikeLiteral first,
                                        IntegerLiteral second, IntegerLiteral third, StringLikeLiteral four) {
        StringBuilder sb = new StringBuilder();
        if (second.getValue() <= 0 || second.getValue() > first.getValue().length()) {
            return first;
        } else {
            if (third.getValue() < 0 || third.getValue() > (first.getValue().length() - third.getValue())) {
                sb.append(first.getValue().substring(0, second.getValue() - 1));
                sb.append(four.getValue());
                return castStringLikeLiteral(first, sb.toString());
            } else {
                sb.append(first.getValue().substring(0, second.getValue() - 1));
                sb.append(four.getValue());
                sb.append(first.getValue().substring(second.getValue()
                        + third.getValue() - 1, first.getValue().length()));
                return castStringLikeLiteral(first, sb.toString());
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
                sb.append(uri.getScheme()); // e.g., http, https
                break;
            case "HOST":
                sb.append(uri.getHost());  // e.g., www.example.com
                break;
            case "PATH":
                sb.append(uri.getPath());  // e.g., /page
                break;
            case "REF":
                try {
                    sb.append(uri.toURL().getRef());  // e.g., /page
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                break;
            case "AUTHORITY":
                sb.append(uri.getAuthority());  // e.g., param1=value1&param2=value2
                break;
            case "FILE":
                try {
                    sb.append(uri.toURL().getFile());  // e.g., param1=value1&param2=value2
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                break;
            case "QUERY":
                sb.append(uri.getQuery());  // e.g., param1=value1&param2=value2
                break;
            case "PORT":
                sb.append(uri.getPort());
                break;
            case "USERINFO":
                sb.append(uri.getUserInfo());  // e.g., user:pass
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
     * Executable arithmetic functions append_trailing_char_if_absent
     */
    @ExecFunction(name = "append_trailing_char_if_absent")
    public static Expression appendTrailingCharIfAbsent(StringLikeLiteral first, StringLikeLiteral second) {
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

        String[] urlParts = first.getValue().split("\\?");
        if (urlParts.length > 1) {
            String query = urlParts[1];
            String[] pairs = query.split("&");

            for (String pair : pairs) {
                String[] keyValue = pair.split("=");
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
        return castStringLikeLiteral(first, first.getValue().replace(second.getValue(), third.getValue()));
    }

}
