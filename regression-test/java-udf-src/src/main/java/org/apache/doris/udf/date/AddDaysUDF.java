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
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/date/AddDaysUDF.java
// and modified by Doris

package org.apache.doris.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

/**
 * Simple date add UDF.
 * Would use the Hive standard function, but that assumes a
 * date format of "YYYY-MM-DD", and we would prefer "YYYYMMDD"
 * and it is too awkward to include lots of substring functions in our hive
 */
public class AddDaysUDF extends UDF {
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

    public String evaluate(String dateStr, int numDays) {
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        DateTime addedDt = dt.plusDays(numDays);
        String addedDtStr = YYYYMMDD.print(addedDt);

        return addedDtStr;
    }
}

