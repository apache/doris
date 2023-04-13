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
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

/**
 * executable function:
 * year, quarter, month, week, dayOfYear, dayOfweek, dayOfMonth, hour, minute, second
 */
public class DateTimeExtract {
    /**
     * Executable datetime extract year
     */
    @ExecFunction(name = "year", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral year(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral year(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral year(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral year(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract quarter
     */
    @ExecFunction(name = "quarter", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral quarter(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral quarter(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral quarter(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral quarter(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract month
     */
    @ExecFunction(name = "month", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral month(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral month(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral month(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral month(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }
}
