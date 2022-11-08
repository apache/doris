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

package org.apache.doris.statistics.util;

public class BlockingCounter {

    private int count = 0;

    private final int upperBound;

    public BlockingCounter(int upperBound) {
        this.upperBound = upperBound;
    }

    public synchronized void incr() {
        while (count >= upperBound) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public synchronized void decr() {
        if (count == 0) {
            return;
        }
        count--;
        notify();
    }

    public long getVal() {
        return count;
    }
}
