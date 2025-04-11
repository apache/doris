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

package org.apache.doris.common;

/**
 * Format options for formatting literals in FE.
 * This is mainly for optional compatibility for Presto/Trino.
 * User can use different session variable "serde_dialect" to choose different format options.
 * This behavior same as in BE. see FormatOptions in be/src/vec/data_types/serde/data_type_serde.h
 */
public class FormatOptions {

    private String nestedStringWrapper;
    private String mapKeyDelim;
    // the string format of null value in complex type
    private String nullFormat;
    private String collectionDelim;
    // isBoolValue = true means the boolean column in collection type(array, map, ...) will print as 0 or 1.
    // false means to print as true/false
    // This is only for boolean column within the collection type.
    // For top level boolean column, it is always 0/1
    private boolean isBoolValueNum;
    // Indicate the nested level of column. It is used to control some behavior of serde
    public int level = 0;

    private FormatOptions(String nestedStringWrapper, String mapKeyDelim, String nullFormat, String collectionDelim,
            boolean isBoolValueNum) {
        this.nestedStringWrapper = nestedStringWrapper;
        this.mapKeyDelim = mapKeyDelim;
        this.nullFormat = nullFormat;
        this.collectionDelim = collectionDelim;
        this.isBoolValueNum = isBoolValueNum;
    }

    public String getNestedStringWrapper() {
        return this.nestedStringWrapper;
    }

    public String getMapKeyDelim() {
        return this.mapKeyDelim;
    }

    public String getNullFormat() {
        return this.nullFormat;
    }

    public String getCollectionDelim() {
        return collectionDelim;
    }

    public boolean isBoolValueNum() {
        return isBoolValueNum;
    }

    public static FormatOptions getDefault() {
        return new FormatOptions("\"", ":", "null", ", ", true);
    }

    public static FormatOptions getForPresto() {
        return new FormatOptions("", "=", "NULL", ", ", true);
    }

    public static FormatOptions getForHive() {
        return new FormatOptions("\"", ":", "null", ",", false);
    }
}
