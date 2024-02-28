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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Converter.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.nereids.PLParser.DtypeContext;
import org.apache.doris.nereids.PLParser.Dtype_lenContext;

/**
 * On-the-fly SQL Converter
 */
public class Converter {

    Exec exec;
    boolean trace = false;

    Converter(Exec e) {
        exec = e;
        trace = exec.getTrace();
    }

    /**
     * Convert a data type
     */
    String dataType(DtypeContext type,
            Dtype_lenContext len) {
        String t = exec.getText(type);
        boolean enclosed = false;
        if (t.charAt(0) == '[') {
            t = t.substring(1, t.length() - 1);
            enclosed = true;
        }
        if (t.equalsIgnoreCase("BIT")) {
            t = "TINYINT";
        } else if (t.equalsIgnoreCase("INT") || t.equalsIgnoreCase("INTEGER")) {
            // MySQL can use INT(n)
        } else if (t.equalsIgnoreCase("INT2")) {
            t = "SMALLINT";
        } else if (t.equalsIgnoreCase("INT4")) {
            t = "INT";
        } else if (t.equalsIgnoreCase("INT8")) {
            t = "BIGINT";
        } else if (t.equalsIgnoreCase("DATETIME") || t.equalsIgnoreCase("SMALLDATETIME")) {
            t = "TIMESTAMP";
        } else if ((t.equalsIgnoreCase("VARCHAR") || t.equalsIgnoreCase("NVARCHAR")) && len.MAX() != null) {
            t = "STRING";
        } else if (t.equalsIgnoreCase("VARCHAR2") || t.equalsIgnoreCase("NCHAR") || t.equalsIgnoreCase("NVARCHAR")
                || t.equalsIgnoreCase("TEXT")) {
            t = "STRING";
        } else if (t.equalsIgnoreCase("NUMBER") || t.equalsIgnoreCase("NUMERIC")) {
            t = "DECIMAL";
            if (len != null) {
                t += exec.getText(len);
            }
        } else if (len != null) {
            if (!enclosed) {
                return exec.getText(type, type.getStart(), len.getStop());
            } else {
                return t + exec.getText(len, len.getStart(), len.getStop());
            }
        } else if (!enclosed) {
            return exec.getText(type, type.getStart(), type.getStop());
        }
        return t;
    }
}
