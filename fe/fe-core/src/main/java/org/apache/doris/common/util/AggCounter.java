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

package org.apache.doris.common.util;

import java.util.LinkedList;

// Counter means indicators field. The counter's name is key, the counter itself is value.
public class AggCounter extends Counter {
    Counter max;
    Counter sum;
    Counter min;
    int number;

    public AggCounter(org.apache.doris.thrift.TUnit type, long value) {
        super(type, value);
        max = new Counter(type, value);
        sum = new Counter(type, value);
        min = new Counter(type, value);
        number = 1;
    }

    public void addCounter(Counter counter) {
        max.maxValue(counter);
        sum.addValue(counter);
        min.minValue(counter);
        number++;
    }

    public void addCounters(LinkedList<Counter> rhsCounter) {
        for (Counter counter : rhsCounter) {
            addCounter(counter);
        }
    }

    public String print() {
        if (isTimeType()) {
            Counter avg = new Counter(sum.getType(), sum.getValue());
            avg.divValue(number);
            String infoString = RuntimeProfile.AVG_TIME_PRE
                    + RuntimeProfile.printCounter(avg.getValue(), avg.getType()) + ", "
                    + RuntimeProfile.MAX_TIME_PRE
                    + RuntimeProfile.printCounter(max.getValue(), max.getType()) + ", "
                    + RuntimeProfile.MIN_TIME_PRE
                    + RuntimeProfile.printCounter(min.getValue(), min.getType());
            return infoString;
        } else {
            String infoString = RuntimeProfile.printCounter(sum.getValue(), sum.getType());
            return infoString;
        }
    }
}
