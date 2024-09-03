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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

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
    @ExecFunction(name = "locate", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression locate(VarcharLiteral first, VarcharLiteral second) {
        return new IntegerLiteral(first.getValue().strip().indexOf(second.getValue()));
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
     * Executable arithmetic functions Hex
     */
    @ExecFunction(name = "hex", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression hexBigInt(BigIntLiteral first) {
        return new VarcharLiteral(Long.toHexString(first.getValue()));
    }

    /**
     * Executable arithmetic functions Hex
     */
    @ExecFunction(name = "hex", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression hexVarchar(VarcharLiteral first) {
        return new VarcharLiteral(Long.toHexString(Long.valueOf(first.getValue())));
    }

    /**
     * Executable arithmetic functions UnHex
     */
    @ExecFunction(name = "unhex", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static Expression unHexVarchar(VarcharLiteral first) {
        return new VarcharLiteral(Long.toBinaryString(Long.valueOf(first.getValue())));
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(name = "concatws", argTypes = {"VARCHAR", "ARRAY"}, returnType = "VARCHAR")
    public static Expression concatWsVarcharArray(VarcharLiteral first, ArrayLiteral second) {
        StringBuilder sb = new StringBuilder();
        for (Literal item : second.getValue()) {
            sb.append(item.getStringValue());
            sb.append(first.getValue());
        }
        return new VarcharLiteral(sb.toString());
    }

    /**
     * Executable arithmetic functions ConcatWs
     */
    @ExecFunction(name = "concatws", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression concatWsVarcharVarchar(VarcharLiteral first, ArrayLiteral second) {
        StringBuilder sb = new StringBuilder();
        for (Literal item : second.getValue()) {
            sb.append(item.getStringValue());
            sb.append(first.getValue());
        }
        return new VarcharLiteral(sb.toString());
    }

}
