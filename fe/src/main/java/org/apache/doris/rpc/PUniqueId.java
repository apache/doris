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

package org.apache.doris.rpc;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import org.apache.doris.thrift.TUniqueId;

@ProtobufClass
public class PUniqueId {

    public PUniqueId() {}
    public PUniqueId(TUniqueId tid) {
        hi = tid.getHi();
        lo = tid.getLo();
    }

    @Protobuf(order = 1, required = true)
    public long hi;
    @Protobuf(order = 2, required = true)
    public long lo;

    @Override
    public int hashCode() {
        return ((int)hi + (int)(hi >> 32) + (int)lo + (int)(lo >> 32));
    }

    @Override
    public String toString() {
        return new StringBuilder().append(hi).append(":").append(lo).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PUniqueId) || obj == null) {
            return false;
        }

        final PUniqueId other = (PUniqueId)obj;
        if (hi != other.hi || lo != other.lo) {
            return false;
        }
        return true;
    }
}
