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

package org.apache.doris.common.profile;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

// Counter means indicators field. The counter's name is key, the counter itself is value.
public class AggCounter extends Counter {
    public Counter max;
    public Counter sum;
    public Counter min;
    int number;

    public AggCounter(org.apache.doris.thrift.TUnit type) {
        super(type, 0);
        max = new Counter(type, 0);
        sum = new Counter(type, 0);
        min = new Counter(type, 0);
        number = 0;
    }

    public void addCounter(Counter counter) {
        if (counter == null) {
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

    public void addCounters(LinkedList<Counter> rhsCounter) {
        for (Counter counter : rhsCounter) {
            addCounter(counter);
        }
    }

    public void mergeCounter(AggCounter rhsAggCounter) {
        if (rhsAggCounter == null) {
            return;
        }
        if (number == 0) {
            max.setValue(rhsAggCounter.max.getValue());
            sum.setValue(rhsAggCounter.sum.getValue());
            min.setValue(rhsAggCounter.min.getValue());
        } else {
            max.maxValue(rhsAggCounter.max);
            sum.addValue(rhsAggCounter.sum);
            min.minValue(rhsAggCounter.min);
        }
        number += rhsAggCounter.number;
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

    /**
     * Convert aggregated counter to a structured map for YAML serialization.
     * For time types: includes avg/max/min (no sum, as time sums are not meaningful)
     * For other types: includes sum/avg/max/min
     * All include count (number of instances) and formatted display string.
     */
    @Override
    public Map<String, Object> toMap() {
        // Use FlowStyleMap to ensure compact single-line output in YAML
        Map<String, Object> map = new FlowStyleMap();
        map.put("unit", sum.getType() != null ? sum.getType().name() : "UNKNOWN");

        if (isTimeType()) {
            // For time types, don't include sum as it's not meaningful
            long avgValue = number > 0 ? sum.getValue() / number : 0;
            map.put("avg", avgValue);
            map.put("max", max.getValue());
            map.put("min", min.getValue());
        } else {
            // For non-time types, include sum
            map.put("sum", sum.getValue());
            long avgValue = number > 0 ? sum.getValue() / number : 0;
            map.put("avg", avgValue);
            map.put("max", max.getValue());
            map.put("min", min.getValue());
        }

        map.put("count", number);
        map.put("display", print());
        return map;
    }

    /**
     * A special Map implementation to mark data that should be output in flow style (single line).
     * This is used for AggCounter data to make YAML output more compact.
     */
    public static class FlowStyleMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }
}
