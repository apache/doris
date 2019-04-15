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

package org.apache.doris.optimizer.base;

public class OptMaxcard {
    long value = Long.MAX_VALUE;
    public OptMaxcard() {
    }
    public OptMaxcard(long value) {
        this.value = value;
    }
    public OptMaxcard(OptMaxcard rhs) {
        this.value = rhs.value;
    }
    public long getValue() { return value; }
    public void add(OptMaxcard rhs) {
        value = Long.min(Long.MAX_VALUE, value + rhs.value);
    }
    public void multiply(OptMaxcard rhs) {
        value = Long.min(Long.MAX_VALUE, value * rhs.value);
    }
    public boolean lessThan(OptMaxcard rhs) {
        return value < rhs.value;
    }
}
