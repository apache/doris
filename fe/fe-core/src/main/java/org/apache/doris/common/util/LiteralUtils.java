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

package org.apache.doris.common.util;

import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class LiteralUtils {

    public static String getStringValue(FloatLiteral literal) {
        if (literal.getType() == Type.TIME || literal.getType() == Type.TIMEV2) {
            // FloatLiteral used to represent TIME type, here we need to remove apostrophe from timeStr
            // for example '11:22:33' -> 11:22:33
            String timeStr = literal.getStringValue();
            return timeStr.substring(1, timeStr.length() - 1);
        } else {
            return BigDecimal.valueOf(literal.getValue()).toPlainString();
        }
    }

    public static String getStringValue(ArrayLiteral literal) {
        List<String> list = new ArrayList<>(literal.getChildren().size());
        literal.getChildren().forEach(v -> {
            if (v instanceof FloatLiteral) {
                list.add(getStringValue((FloatLiteral) v));
            } else if (v instanceof DecimalLiteral) {
                list.add(((DecimalLiteral) v).getValue().toPlainString());
            } else if (v instanceof StringLiteral) {
                list.add("\"" + v.getStringValue() + "\"");
            } else if (v instanceof ArrayLiteral) {
                list.add(getStringValue((ArrayLiteral) v));
            } else {
                list.add(v.getStringValue());
            }
        });
        return "[" + StringUtils.join(list, ", ") + "]";
    }
}
