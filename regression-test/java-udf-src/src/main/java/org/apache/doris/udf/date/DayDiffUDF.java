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
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/date/DayDiffUDF.java
// and modified by Doris

package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormatter;

/**
 * Given two dates in YYYYMMDD,
 * return the number of days between
 * midnight on each day..
 * <p/>
 * day_diff( "20120701", "20120702") == 1
 * day_diff( "20120701", "20120701") == 0
 */
@Description(
    name = "day_diff",
    value = " The difference in days of two YYYYMMDD dates"
)
public class DayDiffUDF extends UDF {
    private static final Logger LOG = Logger.getLogger(DayDiffUDF.class);
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

    public Integer evaluate(String date1Str, String date2Str) {
        DateTime dt1 = YYYYMMDD.parseDateTime(date1Str);
        DateTime dt2 = YYYYMMDD.parseDateTime(date2Str);

        int dayDiff = Days.daysBetween(dt1, dt2).getDays();

        return dayDiff;
    }
}
