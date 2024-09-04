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
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * executable functions:
 * unclassified date function
 */
public class DateTimeAcquire {
    /**
     * date acquire function: now
     */
    @ExecFunction(hasVarArgs = false, name = "now", argTypes = {}, returnType = "DATETIME")
    public static Expression now() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(hasVarArgs = false, name = "now", argTypes = {"INT"}, returnType = "DATETIMEV2")
    public static Expression now(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()),
                precision.getValue());
    }

    /**
     * date acquire function: current_timestamp
     */
    @ExecFunction(hasVarArgs = false, name = "current_timestamp", argTypes = {}, returnType = "DATETIME")
    public static Expression currentTimestamp() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(hasVarArgs = false, name = "current_timestamp", argTypes = {"INT"}, returnType = "DATETIMEV2")
    public static Expression currentTimestamp(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), precision.getValue());
    }

    /**
     * date acquire function: localtime/localtimestamp
     */
    @ExecFunction(hasVarArgs = false, name = "localtime", argTypes = {}, returnType = "DATETIME")
    public static Expression localTime() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(hasVarArgs = false, name = "localtimestamp", argTypes = {}, returnType = "DATETIME")
    public static Expression localTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    /**
     * date acquire function: current_date
     */
    @ExecFunction(hasVarArgs = false, name = "curdate", argTypes = {}, returnType = "DATE")
    public static Expression curDate() {
        return DateLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(hasVarArgs = false, name = "current_date", argTypes = {}, returnType = "DATE")
    public static Expression currentDate() {
        return DateLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    // comment these function temporally until we support TimeLiteral
    // /**
    //  * date acquire function: current_time
    //  */
    // @ExecFunction(hasVarArgs = false, name = "curtime", argTypes = {}, returnType = "TIME")
    // public static Expression curTime() {
    //     return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    // }

    // @ExecFunction(hasVarArgs = false, name = "current_time", argTypes = {}, returnType = "TIME")
    // public static Expression currentTime() {
    //     return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    // }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(hasVarArgs = false, name = "unix_timestamp", argTypes = {}, returnType = "INT")
    public static Expression unixTimestamp() {
        return new IntegerLiteral((int) (System.currentTimeMillis() / 1000L));
    }

    /**
     * date transformation function: utc_timestamp
     */
    @ExecFunction(hasVarArgs = false, name = "utc_timestamp", argTypes = {}, returnType = "INT")
    public static Expression utcTimestamp() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(ZoneId.of("UTC+0")));
    }
}
