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

package org.apache.doris.persist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.doris.common.io.Writable;
import com.google.common.collect.Lists;

public class BackendIdsUpdateInfo implements Writable {
    private List<Long> backendIds;
    
    public BackendIdsUpdateInfo() {
        this.backendIds = Lists.newArrayList();
    }

    public BackendIdsUpdateInfo(List<Long> backends) {
        this.backendIds = backends;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(backendIds.size());
        for (Long id : backendIds) {
            out.writeLong(id);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int backendCount = in.readInt();
        while (backendCount-- > 0) {
            backendIds.add(in.readLong());
        }
    }

    public List<Long> getBackendList() {
        return backendIds;
    }

    public void setBackendList(List<Long> backendList) {
        this.backendIds = backendList;
    } 

}
