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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ToSqlUtils.java
// and modified by Doris

package org.apache.doris.analysis;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Contains utility methods for creating SQL strings, for example,
 * for creating identifier strings that are compatible with Hive or Impala.
 */
public class ToSqlUtils {
    /**
     * Given an unquoted identifier string, returns an identifier lexable by
     * Impala and Hive, possibly by enclosing the original identifier in "`" quotes.
     * For example, Hive cannot parse its own auto-generated column
     * names "_c0", "_c1" etc. unless they are quoted. Impala and Hive keywords
     * must also be quoted.
     *
     * Impala's lexer recognizes a superset of the unquoted identifiers that Hive can.
     * At the same time, Impala's and Hive's list of keywords differ.
     * This method always returns an identifier that Impala and Hive can recognize,
     * although for some identifiers the quotes may not be strictly necessary for
     * one or the other system.
     */
    public static String getIdentSql(String ident) {
        return ident;
    }

    public static List<String> getIdentSqlList(List<String> identList) {
        List<String> identSqlList = Lists.newArrayList();
        for (String ident: identList) {
            identSqlList.add(getIdentSql(ident));
        }
        return identSqlList;
    }
}
