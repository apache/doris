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

package org.apache.doris.load.sync.model;

import org.apache.doris.load.sync.position.PositionRange;

import java.util.List;

// Equivalent to a batch get from server
// T = dataType
// P = positionType
public class Events<T, P> {
    private long id;
    private List<T> datas;
    private PositionRange<P> positionRange;
    private long memSize;

    public Events(Long id) {
        this(id, null);
    }

    public Events(Long id, List<T> datas) {
        this.id = id;
        this.datas = datas;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<T> getDatas() {
        return datas;
    }

    public void setDatas(List<T> datas) {
        this.datas = datas;
    }

    public PositionRange<P> getPositionRange() {
        return positionRange;
    }

    public void setPositionRange(PositionRange<P> positionRange) {
        this.positionRange = positionRange;
    }

    public void setMemSize(Long memSize) {
        this.memSize = memSize;
    }

    public long getMemSize() {
        return this.memSize;
    }
}
