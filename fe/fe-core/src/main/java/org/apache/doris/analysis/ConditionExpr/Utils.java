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
package org.apache.doris.analysis.ConditionExpr;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class Utils
{
    private Utils() {}
    //TODO: support diffrent type
    public static boolean isOrderable(Type type) {
        return true;
    }

    public static boolean isFloatingPointNaN(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type == Type.DOUBLE) {
            return Double.isNaN((double) value);
        }
        if (type == Type.FLOAT) {
            return Float.isNaN(intBitsToFloat(toIntExact((long) value)));
        }
        return false;
    }

    // only support basic type: int, string, date, is null, boolean
    public static boolean isCompatibleType(Type t1, Type t2) {
        if (isDigitalType(t1) && isDigitalType(t2)) {
            return true;
        } else if (isStringType(t1) && isStringType(t2)) {
            return true;
        } else if (isTimeType(t1) && isTimeType(t2)) {
            return true;
        } else if (t1.getPrimitiveType().equals(t2.getPrimitiveType())) {
            return true;
        }
        return false;
    }

    public static boolean isDigitalType(Type type) {
        List<PrimitiveType> digitalType = new ArrayList<>();
        digitalType.add(PrimitiveType.TINYINT);
        digitalType.add(PrimitiveType.SMALLINT);
        digitalType.add(PrimitiveType.INT);
        digitalType.add(PrimitiveType.BIGINT);
        digitalType.add(PrimitiveType.LARGEINT);
        digitalType.add(PrimitiveType.FLOAT);
        digitalType.add(PrimitiveType.DOUBLE);
        if (digitalType.contains(type.getPrimitiveType())) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isStringType(Type type) {
        List<PrimitiveType> stringType = new ArrayList<>();
        stringType.add(PrimitiveType.CHAR);
        stringType.add(PrimitiveType.VARCHAR);
        stringType.add(PrimitiveType.INVALID_TYPE);
        if (stringType.contains(type.getPrimitiveType())) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isTimeType(Type type) {
        List<PrimitiveType> dateType = new ArrayList<>();
        dateType.add(PrimitiveType.DATE);
        dateType.add(PrimitiveType.DATETIME);
        dateType.add(PrimitiveType.TIME);
        if (dateType.contains(type.getPrimitiveType())) {
            return true;
        } else {
            return false;
        }
    }

    public static Type getCompatibleType(Type first, Type second) {
        if (isDigitalType(first)) {
            Map<PrimitiveType, Integer> digitalMap = new HashMap<PrimitiveType, Integer>();
            digitalMap.put(PrimitiveType.TINYINT, 1);
            digitalMap.put(PrimitiveType.SMALLINT, 2);
            digitalMap.put(PrimitiveType.INT, 3);
            digitalMap.put(PrimitiveType.LARGEINT,4);
            digitalMap.put(PrimitiveType.BIGINT, 5);
            if (digitalMap.get(first.getPrimitiveType()) > digitalMap.get(second.getPrimitiveType())) {
                return first;
            } else {
                return second;
            }
        } else if (isStringType(first)) {
            Map<PrimitiveType, Integer> stringMap = new HashMap<PrimitiveType, Integer>();
            stringMap.put(PrimitiveType.CHAR, 1);
            stringMap.put(PrimitiveType.VARCHAR, 2);
            if (stringMap.get(first.getPrimitiveType()) > stringMap.get(second.getPrimitiveType())) {
                return first;
            } else {
                return second;
            }
        } else if (isTimeType(first)) {
            Map<PrimitiveType, Integer> timeMap = new HashMap<PrimitiveType, Integer>();
            timeMap.put(PrimitiveType.TIME, 1);
            timeMap.put(PrimitiveType.DATE, 2);
            timeMap.put(PrimitiveType.DATETIME, 3);
            if (timeMap.get(first.getPrimitiveType()) > timeMap.get(second.getPrimitiveType())) {
                return first;
            } else {
                return second;
            }
        }
        if (first.getPrimitiveType() != PrimitiveType.INVALID_TYPE) {
            return first;
        } else {
            return second;
        }
    }
}
