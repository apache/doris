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
    /**
     * Executable arithmetic functions concat
     */
    @ExecFunction(name = "concat", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression concatVarcharVarchar(VarcharLiteral first, VarcharLiteral second) {
        String result = first.getValue() + second.getValue();
        return new VarcharLiteral(result);
    }

    /**
     * Executable arithmetic functions substring
     */
    @ExecFunction(name = "substring", argTypes = {"VARCHAR", "INT", "INT"}, returnType = "VARCHAR")
    public static Expression substringVarcharIntInt(VarcharLiteral first, IntegerLiteral second, IntegerLiteral third) {
        int stringLength = first.getValue().length();
        if (stringLength == 0) {
            return new VarcharLiteral("");
        }
        int leftIndex = 0;
        if (second.getValue() < (- stringLength)) {
            return new VarcharLiteral("");
        } else if (second.getValue() < 0) {
            leftIndex = stringLength + second.getValue();
        } else if (second.getValue() <= stringLength) {
            leftIndex = second.getValue() - 1;
        } else {
            return new VarcharLiteral("");
        }
        int rightIndex = 0;
        if (third.getValue() <= 0) {
            return new VarcharLiteral("");
        } else if ((third.getValue() + leftIndex) > stringLength) {
            rightIndex = stringLength;
        } else {
            rightIndex = third.getValue() + leftIndex;
        }
        return new VarcharLiteral(first.getValue().substring(leftIndex, rightIndex));
    }

    /**
     * Executable arithmetic functions length
     */
    @ExecFunction(name = "length", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression lengthVarchar(VarcharLiteral first) {
        return new IntegerLiteral(first.getValue().length());
    }

    /**
     * Executable arithmetic functions Lower
     */
    @ExecFunction(name = "lower", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression lowerVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().toLowerCase());
    }

    /**
     * Executable arithmetic functions Upper
     */
    @ExecFunction(name = "upper", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression upperVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().toUpperCase());
    }

    /**
     * Executable arithmetic functions Trim
     */
    @ExecFunction(name = "trim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression trimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().strip());
    }

    /**
     * Executable arithmetic functions LTrim
     */
    @ExecFunction(name = "ltrim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression ltrimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().stripLeading());
    }

    /**
     * Executable arithmetic functions RTrim
     */
    @ExecFunction(name = "rtrim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression rtrimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().stripTrailing());
    }

    /**
     * Executable arithmetic functions Replace
     */
    @ExecFunction(name = "replace", argTypes = {"VARCHAR", "VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression replace(VarcharLiteral first, VarcharLiteral second, VarcharLiteral third) {
        if (second.getValue().length() == 0) {
            return new VarcharLiteral(first.getValue());
        }
        return new VarcharLiteral(first.getValue().replace(second.getValue(), third.getValue()));
    }

    /**
     * Executable arithmetic functions Left
     */
    @ExecFunction(name = "left", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression left(VarcharLiteral first, IntegerLiteral second) {
        if (second.getValue() <= 0) {
            return new VarcharLiteral("");
        } else if (second.getValue() < first.getValue().length()) {
            return new VarcharLiteral(first.getValue().substring(0, second.getValue()));
        } else {
            return first;
        }
    }

    /**
     * Executable arithmetic functions Right
     */
    @ExecFunction(name = "right", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression right(VarcharLiteral first, IntegerLiteral second) {
        if (second.getValue() < (- first.getValue().length()) || Math.abs(second.getValue()) == 0) {
            return new VarcharLiteral("");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            if (second.getValue() > 0) {
                return new VarcharLiteral(first.getValue().substring(
                    first.getValue().length() - second.getValue(), first.getValue().length()));
            } else {
                return new VarcharLiteral(first.getValue().substring(
                    Math.abs(second.getValue()) - 1, first.getValue().length()));
            }
        }
    }

    /**
     * Executable arithmetic functions Locate
     */
    @ExecFunction(name = "locate", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression locate(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(second.getValue().strip().indexOf(first.getValue()) + 1);
    }

    /**
     * Executable arithmetic functions Instr
     */
    @ExecFunction(name = "instr", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression instr(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(first.getValue().indexOf(second.getValue()) + 1);
    }

    /**
     * Executable arithmetic functions Ascii
     */
    @ExecFunction(name = "ascii", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression ascii(VarcharLiteral first) {
        if (first.getValue().length() == 0) {
            return new IntegerLiteral(0);
        }
        char firstChar = first.getValue().charAt(0);
        return new IntegerLiteral(firstChar);
    }

    /**
     * Executable arithmetic functions Bin
     */
    @ExecFunction(name = "bin", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression bin(BigIntLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(first.getValue()));
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(name = "concat_ws", argTypes = {"VARCHAR", "ARRAY<VARCHAR>"}, returnType = "VARCHAR")
    public static Expression concatWsVarcharArray(VarcharLiteral first, ArrayLiteral second) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < second.getValue().size() - 1; i++) {
            if (!(second.getValue().get(i) instanceof NullLiteral)) {
                sb.append(second.getValue().get(i).getValue());
                sb.append(first.getValue());
            }
        }
        sb.append(second.getValue().get(second.getValue().size() - 1).getValue());
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(varArgs = true, name = "concat_ws", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression concatWsVarcharVarchar(VarcharLiteral first, VarcharLiteral... second) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < second.length - 1; i++) {
            sb.append(second[i].getValue());
            sb.append(first.getValue());
        }
        sb.append(second[second.length - 1].getValue());
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions CharacterLength
     */
    @ExecFunction(name = "character_length", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression characterLength(VarcharLiteral first) {
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
    @ExecFunction(name = "initcap", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression initCap(VarcharLiteral first) {
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
        return new VarcharLiteral(result.toString());
    }

    /**
     * Executable arithmetic functions md5
     */
    @ExecFunction(name = "md5", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression md5(VarcharLiteral first) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            // Update the digest with the input bytes
            md.update(first.getValue().getBytes());
            return new VarcharLiteral(bytesToHex(md.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executable arithmetic functions md5
     */
    @ExecFunction(varArgs = true, name = "md5sum", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression md5Sum(VarcharLiteral... first) {
        try {
            // Step 1: Create a MessageDigest instance for MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // Step 2: Concatenate all strings in the list into one string
            StringBuilder combinedInput = new StringBuilder();
            for (VarcharLiteral input : first) {
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
            return new VarcharLiteral(hexString.toString());

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
    @ExecFunction(varArgs = true, name = "field", argTypes = {"INT", "INT"}, returnType = "INT")
    public static Expression fieldInt(IntegerLiteral first, IntegerLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"TINYINT", "TINYINT"}, returnType = "INT")
    public static Expression fieldTinyInt(TinyIntLiteral first, TinyIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static Expression fieldSmallInt(SmallIntLiteral first, SmallIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"BIGINT", "BIGINT"}, returnType = "INT")
    public static Expression fieldBigInt(BigIntLiteral first, BigIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "INT")
    public static Expression fieldLargeInt(LargeIntLiteral first, LargeIntLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"FLOAT", "FLOAT"}, returnType = "INT")
    public static Expression fieldFloat(FloatLiteral first, FloatLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "INT")
    public static Expression fieldDouble(DoubleLiteral first, DoubleLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "INT")
    public static Expression fieldDecimalV2(DecimalLiteral first, DecimalLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"DECIMALV3", "DECIMALV3"}, returnType = "INT")
    public static Expression fieldDecimalV3(DecimalV3Literal first, DecimalV3Literal... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"DATETIME", "DATETIME"}, returnType = "INT")
    public static Expression fieldDateTime(DateTimeLiteral first, DateTimeLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "INT")
    public static Expression fieldDateTimeV2(DateTimeV2Literal first, DateTimeV2Literal... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression fieldVarchar(VarcharLiteral first, VarcharLiteral... second) {
        return new IntegerLiteral(compareLiteral(first, second));
    }

    /**
     * Executable arithmetic functions field
     */
    @ExecFunction(varArgs = true, name = "field", argTypes = {"STRING", "STRING"}, returnType = "INT")
    public static Expression fieldString(StringLiteral first, StringLiteral... second) {
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
    @ExecFunction(name = "find_in_set", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression findInSetVarchar(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(findStringInSet(first.getValue(), second.getValue()));
    }

    /**
     * Executable arithmetic functions find_in_set
     */
    @ExecFunction(name = "find_in_set", argTypes = {"STRING", "STRING"}, returnType = "INT")
    public static Expression findInSetString(StringLiteral first, StringLiteral second) {
        return new IntegerLiteral(findStringInSet(first.getValue(), second.getValue()));
    }

    /**
     * Executable arithmetic functions repeat
     */
    @ExecFunction(name = "repeat", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression repeat(VarcharLiteral first, IntegerLiteral second) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < second.getValue(); i++) {
            sb.append(first.getValue());
        }
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions reverse
     */
    @ExecFunction(name = "reverse", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression reverseVarchar(VarcharLiteral first) {
        StringBuilder sb = new StringBuilder();
        sb.append(first.getValue());
        return new VarcharLiteral(sb.reverse().toString());
    }

    /**
     * Executable arithmetic functions reverse
     */
    @ExecFunction(name = "reverse", argTypes = {"STRING"}, returnType = "STRING")
    public static Expression reverseString(VarcharLiteral first) {
        StringBuilder sb = new StringBuilder();
        sb.append(first.getValue());
        return new VarcharLiteral(sb.reverse().toString());
    }

    /**
     * Executable arithmetic functions space
     */
    @ExecFunction(name = "space", argTypes = {"INT"}, returnType = "VARCHAR")
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
    @ExecFunction(name = "split_by_char", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "ARRAY<VARCHAR>")
    public static Expression splitByChar(VarcharLiteral first, VarcharLiteral second) {
        String[] result = first.getValue().split(second.getValue());
        List<Literal> items = new ArrayList<>();
        for (int i = 1; i < result.length; i++) {
            items.add(new StringLiteral(result[i]));
        }
        return new ArrayLiteral(items);
    }

    /**
     * Executable arithmetic functions split_by_string
     */
    @ExecFunction(name = "split_by_char", argTypes = {"STRING", "STRING"}, returnType = "ARRAY<VARCHAR>")
    public static Expression splitByString(StringLiteral first, StringLiteral second) {
        String[] result = first.getValue().split(second.getValue());
        List<Literal> items = new ArrayList<>();
        for (int i = 1; i < result.length; i++) {
            items.add(new StringLiteral(result[i]));
        }
        return new ArrayLiteral(items);
    }

    /**
     * Executable arithmetic functions split_part
     */
    @ExecFunction(name = "split_part", argTypes = {"VARCHAR", "VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression splitPart(VarcharLiteral literal, VarcharLiteral chr, IntegerLiteral number) {
        if (literal.getValue().equals(chr.getValue())) {
            if (Math.abs(number.getValue()) == 1 || Math.abs(number.getValue()) == 2) {
                return new VarcharLiteral("");
            }
        }
        String separator = chr.getValue();
        String[] parts = null;
        if (number.getValue() < 0) {
            StringBuilder sb = new StringBuilder(literal.getValue());
            StringBuilder seperatorBuilder = new StringBuilder(separator);
            separator = seperatorBuilder.reverse().toString();
            if (".$|()[{^?*+\\".indexOf(separator) != -1 || separator.startsWith("\\")) {
                separator = "\\" + separator;
            }
            parts = sb.reverse().toString().split(separator);
        } else {
            if (".$|()[{^?*+\\".indexOf(separator) != -1 || separator.startsWith("\\")) {
                separator = "\\" + separator;
            }
            parts = literal.getValue().split(separator);
        }

        if (parts.length < Math.abs(number.getValue()) || number.getValue() == 0) {
            if (parts.length == Math.abs(number.getValue()) - 1) {
                if (number.getValue() < 0 && literal.getValue().startsWith(chr.getValue())
                        || number.getValue() > 0 && literal.getValue().endsWith(chr.getValue())) {
                    return new VarcharLiteral("");
                }
            }
            return new NullLiteral();
        } else if (number.getValue() < 0) {
            StringBuilder result = new StringBuilder(parts[Math.abs(number.getValue()) - 1]);
            return new VarcharLiteral(result.reverse().toString());
        } else {
            return new VarcharLiteral(parts[number.getValue() - 1]);
        }
    }

    /**
     * Executable arithmetic functions substring_index
     */
    @ExecFunction(name = "substring_index", argTypes = {"VARCHAR", "VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression substringIndex(VarcharLiteral literal, VarcharLiteral chr, IntegerLiteral number) {
        String[] parts = literal.getValue().split(chr.getValue());
        if (Math.abs(number.getValue()) >= parts.length) {
            return literal;
        }
        int leftIndex;
        int rightIndex;
        if (parts.length < number.getValue() || number.getValue() < (- parts.length) || number.getValue() == 0) {
            return new VarcharLiteral("");
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
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions strcmp
     */
    @ExecFunction(name = "strcmp", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "TINYINT")
    public static Expression strcmp(VarcharLiteral first, VarcharLiteral second) {
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
    @ExecFunction(name = "strleft", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression strLeft(VarcharLiteral first, IntegerLiteral second) {
        if (second.getValue() <= 0) {
            return new VarcharLiteral("");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            return new VarcharLiteral(first.getValue().substring(0, second.getValue()));
        }
    }

    /**
     * Executable arithmetic functions strRight
     */
    @ExecFunction(name = "strright", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression strRight(VarcharLiteral first, IntegerLiteral second) {
        if (second.getValue() < (- first.getValue().length()) || Math.abs(second.getValue()) == 0) {
            return new VarcharLiteral("");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            if (second.getValue() > 0) {
                return new VarcharLiteral(first.getValue().substring(
                    first.getValue().length() - second.getValue(), first.getValue().length()));
            } else {
                return new VarcharLiteral(first.getValue().substring(
                    Math.abs(second.getValue()) - 1, first.getValue().length()));
            }
        }
    }

    /**
     * Executable arithmetic functions overlay
     */
    @ExecFunction(name = "overlay", argTypes = {"VARCHAR", "INT", "INT", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression overlay(VarcharLiteral first,
                                        IntegerLiteral second, IntegerLiteral third, VarcharLiteral four) {
        StringBuilder sb = new StringBuilder();
        if (second.getValue() <= 0 || second.getValue() > first.getValue().length()) {
            return first;
        } else {
            if (third.getValue() < 0 || third.getValue() > (first.getValue().length() - third.getValue())) {
                sb.append(first.getValue().substring(0, second.getValue() - 1));
                sb.append(four.getValue());
                return new VarcharLiteral(sb.toString());
            } else {
                sb.append(first.getValue().substring(0, second.getValue() - 1));
                sb.append(four.getValue());
                sb.append(first.getValue().substring(second.getValue()
                        + third.getValue() - 1, first.getValue().length()));
                return new VarcharLiteral(sb.toString());
            }
        }
    }

    /**
     * Executable arithmetic functions parseurl
     */
    @ExecFunction(name = "parse_url", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression parseurl(VarcharLiteral first, VarcharLiteral second) {
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
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions urldecode
     */
    @ExecFunction(name = "url_decode", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression urlDecode(VarcharLiteral first) {
        try {
            return new VarcharLiteral(URLDecoder.decode(first.getValue(), StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executable arithmetic functions append_trailing_char_if_absent
     */
    @ExecFunction(name = "append_trailing_char_if_absent", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression appendTrailingCharIfAbsent(VarcharLiteral first, VarcharLiteral second) {
        if (first.getValue().endsWith(second.getValue())) {
            return first;
        } else {
            return new VarcharLiteral(first.getValue() + second.getValue());
        }
    }

    /**
     * Executable arithmetic functions endsWith
     */
    @ExecFunction(name = "ends_with", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "BOOLEAN")
    public static Expression endsWith(VarcharLiteral first, VarcharLiteral second) {
        if (first.getValue().endsWith(second.getValue())) {
            return BooleanLiteral.TRUE;
        } else {
            return BooleanLiteral.FALSE;
        }
    }

    /**
     * Executable arithmetic functions extractUrlParameter
     */
    @ExecFunction(name = "extract_url_parameter", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression extractUrlParameter(VarcharLiteral first, VarcharLiteral second) {
        if (first.getValue() == null || first.getValue().indexOf('?') == -1) {
            return new VarcharLiteral("");
        }

        String[] urlParts = first.getValue().split("\\?");
        if (urlParts.length > 1) {
            String query = urlParts[1];
            String[] pairs = query.split("&");

            for (String pair : pairs) {
                String[] keyValue = pair.split("=");
                if (second.getValue().equals(keyValue[0])) {
                    return new VarcharLiteral(keyValue[1]);
                }
            }
        }
        return new VarcharLiteral("");
    }

    /**
     * Executable arithmetic functions quote
     */
    @ExecFunction(name = "quote", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression quote(VarcharLiteral first) {
        return new VarcharLiteral("\'" + first.getValue() + "\'");
    }

    /**
     * Executable arithmetic functions replaceEmpty
     */
    @ExecFunction(name = "replace_empty", argTypes = {"VARCHAR", "VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression replaceEmpty(VarcharLiteral first, VarcharLiteral second, VarcharLiteral third) {
        return new VarcharLiteral(first.getValue().replace(second.getValue(), third.getValue()));
    }

}
