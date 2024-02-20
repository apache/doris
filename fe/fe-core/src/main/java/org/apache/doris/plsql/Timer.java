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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Timer.java
// and modified by Doris

package org.apache.doris.plsql;

public class Timer {
    long start = 0;
    long stop = 0;
    long elapsed = 0;

    /**
     * Start the timer
     */
    public long start() {
        start = System.currentTimeMillis();
        return start;
    }

    /**
     * Get intermediate timer value
     */
    public long current() {
        return System.currentTimeMillis();
    }

    /**
     * Stop the timer and return elapsed time
     */
    public long stop() {
        stop = System.currentTimeMillis();
        elapsed = stop - start;
        return elapsed;
    }

    /**
     * Format the elapsed time
     */
    public String format() {
        if (elapsed < 1000) {
            return String.valueOf(elapsed) + " ms";
        }
        return String.format("%.2f", ((float) elapsed) / 1000) + " sec";
    }
}
