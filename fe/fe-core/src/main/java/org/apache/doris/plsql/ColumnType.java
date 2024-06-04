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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/ColumnType.java
// and modified by Doris

package org.apache.doris.plsql;

public class ColumnType {
    private final String type;
    private final Precision precision;

    public static ColumnType parse(String type) {
        return new ColumnType(parseType(type), Precision.parse(type));
    }

    public ColumnType(String type, Precision precision) {
        this.type = type;
        this.precision = precision;
    }

    private static String parseType(String type) {
        int index = type.indexOf('(');
        return index == -1 ? type : type.substring(0, index);
    }

    public String typeString() {
        return type;
    }

    public Precision precision() {
        return precision;
    }

    private static class Precision {
        public final int len;
        public final int scale;

        public static Precision parse(String type) {
            int open = type.indexOf('(');
            if (open == -1) {
                return new Precision(0, 0);
            }
            int len;
            int scale = 0;
            int comma = type.indexOf(',', open);
            int close = type.indexOf(')', open);
            if (comma == -1) {
                len = Integer.parseInt(type.substring(open + 1, close));
            } else {
                len = Integer.parseInt(type.substring(open + 1, comma));
                scale = Integer.parseInt(type.substring(comma + 1, close));
            }
            return new Precision(scale, len);
        }

        Precision(int scale, int len) {
            this.len = len;
            this.scale = scale;
        }
    }
}
