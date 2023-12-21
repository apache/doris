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

    public AggCounter(org.apache.doris.thrift.TUnit type) {
        super(type, 0);
        max = new Counter(type, 0);
        sum = new Counter(type, 0);
        min = new Counter(type, 0);
        number = 0;
    }

    public AggCounter(org.apache.doris.thrift.TAggCounter tcounter) {
        super(tcounter.getType(), 0);
        max = new Counter(tcounter.getType(), tcounter.max_value);
        sum = new Counter(tcounter.getType(), tcounter.sum_value);
        min = new Counter(tcounter.getType(), tcounter.min_value);
        number = tcounter.number;
    }

    public void setValue(org.apache.doris.thrift.TAggCounter tcounter) {
        setType(tcounter.getType());
        setLevel(tcounter.getLevel());
        max.setValue(tcounter.max_value);
        sum.setValue(tcounter.sum_value);
        min.setValue(tcounter.min_value);
    }

    public void addCounter(Counter counter) {
        if (counter == null) {
            return;
        }
        if (counter instanceof AggCounter) {
            addAggCounter((AggCounter) counter);
            return;
        }
        if (number == 0) {
            max.setValue(counter.getValue());
            sum.setValue(counter.getValue());
            min.setValue(counter.getValue());
        } else {
            max.maxValue(counter);
            sum.addValue(counter);
            min.minValue(counter);
        }
        number++;
    }

    private void addAggCounter(AggCounter counter) {
        this.number += counter.number;
        this.max.maxValue(counter.max);
        this.min.minValue(counter.min);
        this.sum.addValue(counter.sum);
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
            Counter avg = new Counter(sum.getType(), sum.getValue());
            avg.divValue(number);
            String infoString = ""
                    + RuntimeProfile.SUM_TIME_PRE
                    + RuntimeProfile.printCounter(sum.getValue(), sum.getType()) + ", "
                    + RuntimeProfile.AVG_TIME_PRE
                    + RuntimeProfile.printCounter(avg.getValue(), avg.getType()) + ", "
                    + RuntimeProfile.MAX_TIME_PRE
                    + RuntimeProfile.printCounter(max.getValue(), max.getType()) + ", "
                    + RuntimeProfile.MIN_TIME_PRE
                    + RuntimeProfile.printCounter(min.getValue(), min.getType());
            return infoString;
        }
    }
}
