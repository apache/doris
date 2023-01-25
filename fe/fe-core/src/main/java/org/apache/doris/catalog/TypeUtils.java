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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.primitives.Longs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TypeUtils {

    /**
     * Returns true if expr is StringLiteral and can parse to valid type, false
     * otherwise.
     * This function only support LargeInt and BigInt now.
     */
    public static boolean canParseTo(Expr expr, PrimitiveType type) {
        if (expr instanceof StringLiteral) {
            if (type == PrimitiveType.BIGINT) {
                return canParseToBigInt((StringLiteral) expr);
            } else if (type == PrimitiveType.LARGEINT) {
                return canParseToLargeInt((StringLiteral) expr);
            }
        }
        return false;
    }

    /**
     * Returns true if expr can parse to valid BigInt, false otherwise.
     */
    private static boolean canParseToBigInt(StringLiteral expr) {
        String value = ((StringLiteral) expr).getValue();
        return Longs.tryParse(value) != null;
    }

    /**
     * Returns true if expr can parse to valid LargeInt, false otherwise.
     */
    private static boolean canParseToLargeInt(Expr expr) {
        try {
            new LargeIntLiteral(((StringLiteral) expr).getValue());
        } catch (AnalysisException e) {
            return false;
        }
        return true;
    }

    public static void writeScalaType(ScalarType type, DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(type);
        Text.writeString(out, json);
    }

    public static ScalarType readScalaType(DataInput input) throws IOException {
        String json = Text.readString(input);
        return GsonUtils.GSON.fromJson(json, ScalarType.class);
    }
}
