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

package org.apache.doris.cdcclient.common;

public class DorisType {
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String LARGEINT = "LARGEINT";
    // largeint is bigint unsigned in information_schema.COLUMNS
    public static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String HLL = "HLL";
    public static final String BITMAP = "BITMAP";
    public static final String ARRAY = "ARRAY";
    public static final String JSONB = "JSONB";
    public static final String JSON = "JSON";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";
    public static final String VARIANT = "VARIANT";
    public static final String IPV4 = "IPV4";
    public static final String IPV6 = "IPV6";
}
