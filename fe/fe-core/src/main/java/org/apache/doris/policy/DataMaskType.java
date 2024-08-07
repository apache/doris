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

package org.apache.doris.policy;

import lombok.Getter;

@Getter
public enum DataMaskType {
    MASK_REDACT("Replace lowercase with 'x', uppercase with 'X', digits with '0'",
        "regexp_replace(regexp_replace(regexp_replace({col},'([A-Z])', 'X'),'([a-z])','x'),'([0-9])','0')"),
    MASK_SHOW_LAST_4("Show last 4 characters; replace rest with 'X'",
        "LPAD(RIGHT({col}, 4), CHAR_LENGTH({col}), 'X')"),
    MASK_SHOW_FIRST_4("Show first 4 characters; replace rest with 'x'",
        "RPAD(LEFT({col}, 4), CHAR_LENGTH({col}), 'X')"),
    MASK_HASH("Hash the value of a varchar with sha256",
        "hex(sha2({col}, 256))"),
    MASK_NULL("Replace with NULL", "NULL"),
    MASK_DATE_SHOW_YEAR("Date: show only year",
        "date_trunc({col}, 'year')"),
    MASK_DEFAULT("Replace with data type default",
        "");

    private final String desc;
    private final String transformer;

    DataMaskType(String desc, String transformer) {
        this.desc = desc;
        this.transformer = transformer;
    }
}
