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

package org.apache.doris.mysql.rbac;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PrivBitSet;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class PrivEntry implements Comparable<PrivEntry>, Writable {

    protected PrivBitSet privSet;

    private UserIdentity userIdentity;

    protected PrivEntry() {
    }

    protected PrivEntry(PrivBitSet privSet) {
        this.privSet = privSet;
    }

    public PrivBitSet getPrivSet() {
        return privSet;
    }

    public void setPrivSet(PrivBitSet privSet) {
        this.privSet = privSet;
    }


    public abstract boolean keyMatch(PrivEntry other);


    public static PrivEntry read(DataInput in) throws IOException {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public int compareTo(PrivEntry o) {
        throw new NotImplementedException();
    }

    /**
     * Help derived classes compare in the order of 'user', 'host', 'catalog', 'db', 'ctl'.
     * Compare strings[i] with strings[i+1] successively, return if the comparison value is not 0 in current loop.
     */
    protected static int compareAssist(String... strings) {
        Preconditions.checkState(strings.length % 2 == 0);
        for (int i = 0; i < strings.length; i += 2) {
            int res = strings[i].compareTo(strings[i + 1]);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }
}
