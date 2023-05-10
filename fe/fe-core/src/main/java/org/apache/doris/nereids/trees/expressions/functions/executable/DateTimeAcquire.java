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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import java.time.LocalDateTime;
import java.util.TimeZone;

/**
 * executable functions:
 * unclassified date function
 */
public class DateTimeAcquire {
    /**
     * date acquire function: now
     */
    @ExecFunction(name = "now", argTypes = {}, returnType = "DATETIME")
    public static Expression now() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now());
    }

    @ExecFunction(name = "now", argTypes = {"INT"}, returnType = "DATETIMEV2")
    public static Expression now(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(),
                precision.getValue());
    }

    /**
     * date acquire function: current_timestamp
     */
    @ExecFunction(name = "current_timestamp", argTypes = {}, returnType = "DATETIME")
    public static Expression currentTimestamp() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now());
    }

    @ExecFunction(name = "current_timestamp", argTypes = {"INT"}, returnType = "DATETIMEV2")
    public static Expression currentTimestamp(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(), precision.getValue());
    }

    /**
     * date acquire function: localtime/localtimestamp
     */
    @ExecFunction(name = "localtime", argTypes = {}, returnType = "DATETIME")
    public static Expression localTime() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(TimeZone.getDefault().toZoneId()));
    }

    @ExecFunction(name = "localtimestamp", argTypes = {}, returnType = "DATETIME")
    public static Expression localTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(TimeZone.getDefault().toZoneId()));
    }

    /**
     * date acquire function: current_date
     */
    @ExecFunction(name = "curdate", argTypes = {}, returnType = "DATE")
    public static Expression curDate() {
        return DateLiteral.fromJavaDateType(LocalDateTime.now());
    }

    @ExecFunction(name = "current_date", argTypes = {}, returnType = "DATE")
    public static Expression currentDate() {
        return DateLiteral.fromJavaDateType(LocalDateTime.now());
    }

    /**
     * date acquire function: current_time
     */
    @ExecFunction(name = "curtime", argTypes = {}, returnType = "DATETIME")
    public static Expression curTime() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now());
    }

    @ExecFunction(name = "current_time", argTypes = {}, returnType = "DATETIME")
    public static Expression currentTime() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now());
    }
}
