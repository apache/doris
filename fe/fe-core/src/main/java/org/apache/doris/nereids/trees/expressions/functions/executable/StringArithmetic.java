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
import org.apache.doris.nereids.trees.expressions.literal.JsonLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * executable functions:
 * concat
 */
public class StringArithmetic {
    /**
     * Executable arithmetic functions concat
     */
    @ExecFunction(varArgs = false, name = "concat", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression concatVarcharVarchar(VarcharLiteral first, VarcharLiteral second) {
        String result = first.getValue() + second.getValue();
        return new VarcharLiteral(result);
    }

    /**
     * Executable arithmetic functions substring
     */
    @ExecFunction(varArgs = false, name = "substring", argTypes = {"VARCHAR", "INT", "INT"}, returnType = "VARCHAR")
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
        } else if (second.getValue() < stringLength) {
            leftIndex = second.getValue();
        } else {
            return new VarcharLiteral("");
        }
        int rightIndex = 0;
        if (third.getValue() < 0) {
            return new VarcharLiteral("");
        } else if (third.getValue() > stringLength) {
            rightIndex = stringLength;
        } else {
            rightIndex = third.getValue();
        }
        return new VarcharLiteral(first.getValue().substring(leftIndex, rightIndex));
    }

    /**
     * Executable arithmetic functions length
     */
    @ExecFunction(varArgs = false, name = "length", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression lengthVarchar(VarcharLiteral first) {
        return new IntegerLiteral(first.getValue().length());
    }

    /**
     * Executable arithmetic functions Lower
     */
    @ExecFunction(varArgs = false, name = "lower", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression lowerVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().toLowerCase());
    }

    /**
     * Executable arithmetic functions Upper
     */
    @ExecFunction(varArgs = false, name = "upper", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression upperVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().toUpperCase());
    }

    /**
     * Executable arithmetic functions Trim
     */
    @ExecFunction(varArgs = false, name = "trim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression trimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().strip());
    }

    /**
     * Executable arithmetic functions LTrim
     */
    @ExecFunction(varArgs = false, name = "ltrim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression ltrimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().stripLeading());
    }

    /**
     * Executable arithmetic functions RTrim
     */
    @ExecFunction(varArgs = false, name = "rtrim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression rtrimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().stripTrailing());
    }

    /**
     * Executable arithmetic functions Replace
     */
    @ExecFunction(varArgs = false, name = "replace",
            argTypes = {"VARCHAR", "VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression replace(VarcharLiteral first, VarcharLiteral second, VarcharLiteral third) {
        if (second.getValue().length() == 0) {
            return new VarcharLiteral(first.getValue());
        }
        return new VarcharLiteral(first.getValue().replace(second.getValue(), third.getValue()));
    }

    /**
     * Executable arithmetic functions Left
     */
    @ExecFunction(varArgs = false, name = "left", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "right", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression right(VarcharLiteral first, IntegerLiteral second) {
        if (second.getValue() <= 0) {
            return new VarcharLiteral("");
        } else if (second.getValue() < first.getValue().length()) {
            int length = first.getValue().length();
            return new VarcharLiteral(first.getValue().substring(length - second.getValue(), length));
        } else {
            return first;
        }
    }

    /**
     * Executable arithmetic functions Locate
     */
    @ExecFunction(varArgs = false, name = "locate", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression locate(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(first.getValue().strip().indexOf(second.getValue()));
    }

    /**
     * Executable arithmetic functions Instr
     */
    @ExecFunction(varArgs = false, name = "instr", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression instr(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(first.getValue().indexOf(second.getValue()) + 1);
    }

    /**
     * Executable arithmetic functions Ascii
     */
    @ExecFunction(varArgs = false, name = "ascii", argTypes = {"VARCHAR"}, returnType = "INT")
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
    @ExecFunction(varArgs = false, name = "bin", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression bin(BigIntLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(first.getValue()));
    }

    /**
     * Executable arithmetic functions Hex
     */
    @ExecFunction(varArgs = false, name = "hex", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression hexBigInt(BigIntLiteral first) {
        return new VarcharLiteral(Long.toHexString(first.getValue()));
    }

    /**
     * Executable arithmetic functions Hex
     */
    @ExecFunction(varArgs = false, name = "hex", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression hexVarchar(VarcharLiteral first) {
        return new VarcharLiteral(Long.toHexString(Long.valueOf(first.getValue())));
    }

    /**
     * Executable arithmetic functions UnHex
     */
    @ExecFunction(varArgs = false, name = "unhex", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression unHexVarchar(VarcharLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(Long.valueOf(first.getValue())));
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(varArgs = false, name = "concat_ws", argTypes = {"VARCHAR", "ARRAY<VARCHAR>"}, returnType = "VARCHAR")
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
     * Executable arithmetic functions Char
     */
    @ExecFunction(varArgs = true, name = "char", argTypes = {"STRING", "INT"}, returnType = "STRING")
    public static Expression charStringInt(StringLiteral first, IntegerLiteral... second) {
        StringBuilder sb = new StringBuilder();
        for (IntegerLiteral item : second) {
            int itemValue = item.getValue();
            byte[] itemBytes = new byte[]{(byte) itemValue};
            try {
                String itemString = new String(itemBytes, first.getValue());
                sb.append(itemString);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return new StringLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions CharacterLength
     */
    @ExecFunction(varArgs = false, name = "character_length", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression characterLength(VarcharLiteral first) {
        return new IntegerLiteral(first.getValue().length());
    }

    /**
     * Executable arithmetic functions initCap
     */
    @ExecFunction(varArgs = false, name = "initcap", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression initCap(VarcharLiteral first) {
        StringBuilder result = new StringBuilder(first.getValue().length());
        boolean capitalizeNext = true;

        for (char c : first.getValue().toCharArray()) {
            if (Character.isWhitespace(c)) {
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
    @ExecFunction(varArgs = false, name = "md5", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
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

    private static Object jsonExtractUnit(JSONObject jsonObject, String unitKey) {
        String[] keys = unitKey.replace("$.", "").split("\\.");
        Object currentObject = jsonObject;
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];

            if (key.endsWith("]")) { // Handle array index access
                int arrayStartIndex = key.indexOf('[');
                String arrayKey = key.substring(0, arrayStartIndex);
                int arrayIndex = Integer.parseInt(key.substring(arrayStartIndex + 1, key.length() - 1));

                if (currentObject instanceof JSONObject) {
                    currentObject = ((JSONObject) currentObject).getJSONArray(arrayKey).get(arrayIndex);
                }
            } else {
                if (currentObject instanceof JSONObject) {
                    currentObject = ((JSONObject) currentObject).get(key);
                }
            }
        }
        return currentObject;
    }

    private static List<String> jsonExtract(String jsonString, String... jsonPaths) {
        List<String> extractedValues = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(jsonString);

        for (String jsonPath : jsonPaths) {
            Object currentObject = jsonExtractUnit(jsonObject, jsonPath);
            extractedValues.add(currentObject.toString());
        }

        return extractedValues;
    }

    /**
     * Executable arithmetic functions jsonContains
     */
    @ExecFunction(varArgs = true, name = "json_extract", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression jsonExtract(VarcharLiteral first, VarcharLiteral... second) {
        StringBuilder sb = new StringBuilder();
        List<String> keys = new ArrayList<>();
        for (VarcharLiteral item : second) {
            keys.add(item.getValue());
        }
        List<String> extractedValues = jsonExtract(first.getValue(), keys.toArray(new String[0]));
        sb.append(extractedValues);
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions jsonb_extract_string
     */
    @ExecFunction(varArgs = false, name = "jsonb_extract_string", argTypes = {"JSON", "VARCHAR"}, returnType = "STRING")
    public static Expression jsonbExtractstring(JsonLiteral first, VarcharLiteral second) {
        StringBuilder sb = new StringBuilder();
        JSONObject jsonObject = new JSONObject(first.getValue());
        Object currentObject = jsonExtractUnit(jsonObject, second.getValue());
        sb.append(currentObject.toString());
        return new StringLiteral(sb.toString());
    }

    /**
     * Checks if a JSON string contains a specific value at a specified path or anywhere in the JSON.
     * @param jsonString the input JSON string
     * @param value the value to check for
     * @param jsonPath (optional) the path to check the value at
     * @return true if the JSON contains the value, false otherwise
     */
    private static boolean jsonContains(String jsonString, String value, String jsonPath) {
        Object jsonObject = null;
        // Determine if the input is a JSONArray or JSONObject
        if (jsonString.trim().startsWith("[")) {
            jsonObject = new JSONArray(jsonString); // Input is a JSONArray
        } else {
            jsonObject = new JSONObject(jsonString);
        }

        if (!jsonPath.equals("$")) {
            // If a JSON path is provided, search at that specific path
            String[] keys = jsonPath.replace("$.", "").split("\\.");
            Object currentObject = jsonObject;

            for (int i = 0; i < keys.length; i++) {
                String key = keys[i];

                if (key.endsWith("]")) { // Handle array index access
                    int arrayStartIndex = key.indexOf('[');
                    String arrayKey = key.substring(0, arrayStartIndex);
                    int arrayIndex = Integer.parseInt(key.substring(arrayStartIndex + 1, key.length() - 1));

                    if (currentObject instanceof JSONObject) {
                        JSONArray jsonArray = ((JSONObject) currentObject).getJSONArray(arrayKey);
                        if (i == keys.length - 1) {
                            // At the last key in the path, check if the value matches the array element
                            return jsonArray.get(arrayIndex).toString().equals(value);
                        } else {
                            currentObject = jsonArray.get(arrayIndex);
                        }
                    }
                } else {
                    if (currentObject instanceof JSONObject) {
                        if (!((JSONObject) currentObject).has(key)) {
                            return false;
                        }
                        currentObject = ((JSONObject) currentObject).get(key);
                    }
                }
            }
            return currentObject.toString().equals(value);
        } else {
            // If no JSON path is provided, search for the value anywhere in the JSON
            return jsonContainsAnywhere(jsonObject, value);
        }
    }

    /**
     * Recursively checks if the value exists anywhere in the JSON object.
     * @param jsonObject the input JSONObject
     * @param value the value to check for
     * @return true if the value is found, false otherwise
     */
    private static boolean jsonContainsAnywhere(Object jsonObject, String value) {
        if (jsonObject.toString().equals(value)) {
            return true;
        }
        if (jsonObject instanceof JSONObject) {
            JSONObject json = (JSONObject) jsonObject;
            for (String key : json.keySet()) {
                Object currentValue = json.get(key);
                if (jsonContainsAnywhere(currentValue, value)) {
                    return true;
                }
            }
        } else if (jsonObject instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) jsonObject;
            for (int i = 0; i < jsonArray.length(); i++) {
                if (jsonContainsAnywhere(jsonArray.get(i), value)) {
                    return true;
                }
            }
        } else {
            return jsonObject.toString().equals(value);
        }
        return false;
    }

    /**
     * Executable arithmetic functions jsonContains
     */
    @ExecFunction(varArgs = false, name = "json_contains",
            argTypes = {"JSON", "JSON", "STRING"}, returnType = "BOOLEAN")
    public static Expression jsonContainsString(JsonLiteral first, JsonLiteral second, StringLiteral third) {
        if (jsonContains(first.getValue(), second.getValue(), third.getValue())) {
            return BooleanLiteral.TRUE;
        } else {
            return BooleanLiteral.FALSE;
        }
    }

    /**
     * Executable arithmetic functions jsonContains
     */
    @ExecFunction(varArgs = false, name = "json_length", argTypes = {"JSON", "VARCHAR"}, returnType = "INT")
    public static Expression jsonLengthVarchar(JsonLiteral first, VarcharLiteral second) {
        if (second.getValue().equals("$")) {
            return new IntegerLiteral(first.getValue().length());
        }
        Object jsonObject = null;
        // Determine if the input is a JSONArray or JSONObject
        if (second.getValue().trim().startsWith("[")) {
            jsonObject = new JSONArray(second.getValue()); // Input is a JSONArray
        } else {
            jsonObject = new JSONObject(second.getValue());
        }
        return new IntegerLiteral(jsonObject.toString().length());
    }

    /**
     * Executable arithmetic functions jsonContains
     */
    @ExecFunction(varArgs = false, name = "json_length", argTypes = {"JSON", "STRING"}, returnType = "INT")
    public static Expression jsonLengthString(JsonLiteral first, StringLiteral second) {
        if (second.getValue().equals("$")) {
            return new IntegerLiteral(first.getValue().length());
        }
        Object jsonObject = null;
        // Determine if the input is a JSONArray or JSONObject
        if (second.getValue().trim().startsWith("[")) {
            jsonObject = new JSONArray(second.getValue()); // Input is a JSONArray
        } else {
            jsonObject = new JSONObject(second.getValue());
        }
        return new IntegerLiteral(jsonObject.toString().length());
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
    @ExecFunction(varArgs = false, name = "find_in_set", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression findInSetVarchar(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(findStringInSet(first.getValue(), second.getValue()));
    }

    /**
     * Executable arithmetic functions find_in_set
     */
    @ExecFunction(varArgs = false, name = "find_in_set", argTypes = {"STRING", "STRING"}, returnType = "INT")
    public static Expression findInSetString(StringLiteral first, StringLiteral second) {
        return new IntegerLiteral(findStringInSet(first.getValue(), second.getValue()));
    }

    /**
     * Executable arithmetic functions repeat
     */
    @ExecFunction(varArgs = false, name = "repeat", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "reverse", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression reverseVarchar(VarcharLiteral first) {
        StringBuilder sb = new StringBuilder();
        sb.append(first.getValue());
        return new VarcharLiteral(sb.reverse().toString());
    }

    /**
     * Executable arithmetic functions reverse
     */
    @ExecFunction(varArgs = false, name = "reverse", argTypes = {"STRING"}, returnType = "STRING")
    public static Expression reverseString(VarcharLiteral first) {
        StringBuilder sb = new StringBuilder();
        sb.append(first.getValue());
        return new VarcharLiteral(sb.reverse().toString());
    }

    /**
     * Executable arithmetic functions space
     */
    @ExecFunction(varArgs = false, name = "space", argTypes = {"INT"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "split_by_char",
            argTypes = {"STRING", "STRING"}, returnType = "ARRAY<VARCHAR>")
    public static Expression splitByChar(StringLiteral first, StringLiteral second) {
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
    @ExecFunction(varArgs = false, name = "split_by_char",
            argTypes = {"STRING", "STRING"}, returnType = "ARRAY<VARCHAR>")
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
    @ExecFunction(varArgs = false, name = "split_part",
            argTypes = {"VARCHAR", "VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression splitPart(VarcharLiteral literal, VarcharLiteral chr, IntegerLiteral number) {
        String[] parts = literal.getValue().split(chr.getValue());
        if (parts.length < number.getValue() || number.getValue() < (- parts.length) || number.getValue() == 0) {
            return new VarcharLiteral("");
        } else if (number.getValue() < 0) {
            return new VarcharLiteral(parts[parts.length + number.getValue()]);
        } else {
            return new VarcharLiteral(parts[number.getValue() - 1]);
        }
    }

    /**
     * Executable arithmetic functions substring_index
     */
    @ExecFunction(varArgs = false, name = "substring_index",
            argTypes = {"VARCHAR", "VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression substringIndex(VarcharLiteral literal, VarcharLiteral chr, IntegerLiteral number) {
        String[] parts = literal.getValue().split(chr.getValue());
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
    @ExecFunction(varArgs = false, name = "strcmp", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "TINYINT")
    public static Expression strcmp(VarcharLiteral first, VarcharLiteral second) {
        if (first.getValue().equals(second.getValue())) {
            return new TinyIntLiteral((byte) 1);
        } else {
            return new TinyIntLiteral((byte) 0);
        }
    }

    /**
     * Executable arithmetic functions strLeft
     */
    @ExecFunction(varArgs = false, name = "strleft", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "strright", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
    public static Expression strRight(VarcharLiteral first, IntegerLiteral second) {
        if (second.getValue() <= 0) {
            return new VarcharLiteral("");
        } else if (second.getValue() > first.getValue().length()) {
            return first;
        } else {
            return new VarcharLiteral(first.getValue().substring(
                    first.getValue().length() - second.getValue(), first.getValue().length()));
        }
    }

    /**
     * Executable arithmetic functions overlay
     */
    @ExecFunction(varArgs = false, name = "overlay",
            argTypes = {"VARCHAR", "INT", "INT", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression overlay(VarcharLiteral first,
                                        IntegerLiteral second, IntegerLiteral third, VarcharLiteral four) {
        StringBuilder sb = new StringBuilder();
        if (second.getValue() <= 0 || second.getValue() > first.getValue().length()) {
            return new VarcharLiteral("");
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
    @ExecFunction(varArgs = false, name = "parse_url", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "url_decode", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression urlDecode(VarcharLiteral first) {
        try {
            return new VarcharLiteral(URLDecoder.decode(first.getValue(), StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executable arithmetic functions decodeasvarchar
     */
    @ExecFunction(varArgs = false, name = "decode_as_varchar", argTypes = {"SMALLINT"}, returnType = "VARCHAR")
    public static Expression decodeasvarcharSmallint(SmallIntLiteral first) {
        return new VarcharLiteral(first.getValue().toString());
    }

    /**
     * Executable arithmetic functions decodeasvarchar
     */
    @ExecFunction(varArgs = false, name = "decode_as_varchar", argTypes = {"INT"}, returnType = "VARCHAR")
    public static Expression decodeasvarcharInt(IntegerLiteral first) {
        return new VarcharLiteral(first.getValue().toString());
    }

    /**
     * Executable arithmetic functions decodeasvarchar
     */
    @ExecFunction(varArgs = false, name = "decode_as_varchar", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression decodeasvarcharBigint(BigIntLiteral first) {
        return new VarcharLiteral(first.getValue().toString());
    }

    /**
     * Executable arithmetic functions decodeasvarchar
     */
    @ExecFunction(varArgs = false, name = "decode_as_varchar", argTypes = {"LARGEINT"}, returnType = "VARCHAR")
    public static Expression decodeasvarcharLargeint(LargeIntLiteral first) {
        return new VarcharLiteral(first.getValue().toString());
    }

    /**
     * Executable arithmetic functions append_trailing_char_if_absent
     */
    @ExecFunction(varArgs = false, name = "append_trailing_char_if_absent",
            argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "ends_with", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "BOOLEAN")
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
    @ExecFunction(varArgs = false, name = "extract_url_parameter",
            argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(varArgs = false, name = "quote", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression quote(VarcharLiteral first) {
        return new VarcharLiteral("\'" + first.getValue() + "\'");
    }

    /**
     * Executable arithmetic functions toBase64
     */
    @ExecFunction(varArgs = false, name = "to_base64", argTypes = {"STRING"}, returnType = "STRING")
    public static Expression toBase64(VarcharLiteral first) {
        // Convert the string to bytes
        byte[] strBytes = first.getValue().getBytes();
        // Encode the bytes to Base64
        String encodedStr = Base64.getEncoder().encodeToString(strBytes);
        return new VarcharLiteral(encodedStr);
    }

    /**
     * Executable arithmetic functions replaceEmpty
     */
    @ExecFunction(varArgs = false, name = "replace_empty",
            argTypes = {"VARCHAR", "VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression replaceEmpty(VarcharLiteral first, VarcharLiteral second, VarcharLiteral third) {
        return new VarcharLiteral(first.getValue().replace(second.getValue(), third.getValue()));
    }

}
