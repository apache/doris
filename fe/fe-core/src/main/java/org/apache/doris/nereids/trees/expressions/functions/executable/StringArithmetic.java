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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.*;
import org.json.JSONObject;

import javax.validation.constraints.Null;
import java.io.UnsupportedEncodingException;
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
    @ExecFunction(hasVarArgs = false, name = "concat", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression concatVarcharVarchar(VarcharLiteral first, VarcharLiteral second) {
        String result = first.getValue() + second.getValue();
        return new VarcharLiteral(result);
    }

    /**
     * Executable arithmetic functions substring
     */
    @ExecFunction(hasVarArgs = false, name = "substring", argTypes = {"VARCHAR", "INT", "INT"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = false, name = "length", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression lengthVarchar(VarcharLiteral first) {
        return new IntegerLiteral(first.getValue().length());
    }

    /**
     * Executable arithmetic functions Lower
     */
    @ExecFunction(hasVarArgs = false, name = "lower", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression lowerVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().toLowerCase());
    }

    /**
     * Executable arithmetic functions Upper
     */
    @ExecFunction(hasVarArgs = false, name = "upper", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression upperVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().toUpperCase());
    }

    /**
     * Executable arithmetic functions Trim
     */
    @ExecFunction(hasVarArgs = false, name = "trim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression trimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().strip());
    }

    /**
     * Executable arithmetic functions LTrim
     */
    @ExecFunction(hasVarArgs = false, name = "ltrim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression ltrimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().stripLeading());
    }

    /**
     * Executable arithmetic functions RTrim
     */
    @ExecFunction(hasVarArgs = false, name = "rtrim", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression rtrimVarchar(VarcharLiteral first) {
        return new VarcharLiteral(first.getValue().stripTrailing());
    }

    /**
     * Executable arithmetic functions Replace
     */
    @ExecFunction(hasVarArgs = false, name = "replace", argTypes = {"VARCHAR", "VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression replace(VarcharLiteral first, VarcharLiteral second, VarcharLiteral third) {
        if (second.getValue().length() == 0) {
            return new VarcharLiteral(first.getValue());
        }
        return new VarcharLiteral(first.getValue().replace(second.getValue(), third.getValue()));
    }

    /**
     * Executable arithmetic functions Left
     */
    @ExecFunction(hasVarArgs = false, name = "left", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = false, name = "right", argTypes = {"VARCHAR", "INT"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = false, name = "locate", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression locate(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(first.getValue().strip().indexOf(second.getValue()));
    }

    /**
     * Executable arithmetic functions Instr
     */
    @ExecFunction(hasVarArgs = false, name = "instr", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression instr(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(first.getValue().indexOf(second.getValue()) + 1);
    }

    /**
     * Executable arithmetic functions Ascii
     */
    @ExecFunction(hasVarArgs = false, name = "ascii", argTypes = {"VARCHAR"}, returnType = "INT")
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
    @ExecFunction(hasVarArgs = false, name = "bin", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression bin(BigIntLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(first.getValue()));
    }

    /**
     * Executable arithmetic functions Hex
     */
    @ExecFunction(hasVarArgs = false, name = "hex", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression hexBigInt(BigIntLiteral first) {
        return new VarcharLiteral(Long.toHexString(first.getValue()));
    }

    /**
     * Executable arithmetic functions Hex
     */
    @ExecFunction(hasVarArgs = false, name = "hex", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression hexVarchar(VarcharLiteral first) {
        return new VarcharLiteral(Long.toHexString(Long.valueOf(first.getValue())));
    }

    /**
     * Executable arithmetic functions UnHex
     */
    @ExecFunction(hasVarArgs = false, name = "unhex", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression unHexVarchar(VarcharLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(Long.valueOf(first.getValue())));
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(hasVarArgs = false, name = "concat_ws", argTypes = {"VARCHAR", "ARRAY<VARCHAR>"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = true, name = "concat_ws", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = true, name = "char", argTypes = {"STRING", "INT"}, returnType = "STRING")
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
    @ExecFunction(hasVarArgs = false, name = "character_length", argTypes = {"VARCHAR"}, returnType = "INT")
    public static Expression characterLength(VarcharLiteral first) {
        return new IntegerLiteral(first.getValue().length());
    }

    /**
     * Executable arithmetic functions initCap
     */
    @ExecFunction(hasVarArgs = false, name = "initcap", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = false, name = "md5", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = true, name = "md5sum", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = true, name = "json_extract", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
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
    @ExecFunction(hasVarArgs = false, name = "jsonb_extract_string", argTypes = {"JSON", "VARCHAR"}, returnType = "STRING")
    public static Expression jsonbExtractstring(JsonLiteral first, VarcharLiteral second) {
        StringBuilder sb = new StringBuilder();
        JSONObject jsonObject = new JSONObject(first.getValue());
        Object currentObject = jsonExtractUnit(jsonObject, second.getValue());
        sb.append(currentObject.toString());
        return new StringLiteral(sb.toString());
    }




    /**
     * Executable arithmetic functions jsonContains
     */
    @ExecFunction(hasVarArgs = false, name = "json_contains", argTypes = {"JSON", "JSON", "VARCHAR"}, returnType = "BOOLEAN")
    public static Expression jsonContains(JsonLiteral first, JsonLiteral second, VarcharLiteral third) {
        return BooleanLiteral.TRUE;
    }

}
