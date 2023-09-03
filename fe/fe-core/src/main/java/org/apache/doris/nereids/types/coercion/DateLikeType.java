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

package org.apache.doris.nereids.types.coercion;

import java.time.temporal.ChronoUnit;
import java.util.Calendar;

/**
 * date like type.
 */
public abstract class DateLikeType extends PrimitiveType {
    private Calendar toCalendar(double d) {
        //d = (year * 10000 + month * 100 + day) * 1000000L;
        int date = (int) (d / 1000000);
        int day = date % 100;
        int month = (date / 100) % 100;
        int year = date / 10000;
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        return calendar;
    }

    @Override
    public double rangeLength(double high, double low) {
        Calendar to = toCalendar(high);
        Calendar from = toCalendar(low);
        return ChronoUnit.DAYS.between(from.toInstant(), to.toInstant());
    }
}
