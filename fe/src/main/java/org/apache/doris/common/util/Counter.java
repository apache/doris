// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

import org.apache.doris.thrift.TUnit;

// Counter means indicators field. The counter's name is key, the counter itself is value.  
public class Counter {
    private long value;
    private TUnit type;
    
    public long getValue() {
        return value;
    }

    public void setValue(long newValue) {
        value = newValue;
    }

    public TUnit getType() {
        return type;
    }

    public void setType(TUnit type) {
        this.type = type;
    }

    public Counter(TUnit type, long value) {
        this.type = type;
        this.value = value;
    }
}
