// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.mysql.privilege;

import com.baidu.palo.analysis.CompoundPredicate.Operator;
import com.baidu.palo.common.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

// ....0000000000
//        ^     ^
//        |     |
//        |     -- first priv(0)
//        |--------last priv(7)
public class PrivBitSet implements Writable {

    private long set = 0;

    public PrivBitSet() {
    }

    public void set(int index) {
        Preconditions.checkState(index < PaloPrivilege.privileges.length, index);
        set |= 1 << index;
    }

    public void unset(int index) {
        Preconditions.checkState(index < PaloPrivilege.privileges.length, index);
        set &= ~set;
    }

    public boolean get(int index) {
        Preconditions.checkState(index < PaloPrivilege.privileges.length, index);
        return (set & (1 << index)) > 0;
    }

    public void or(PrivBitSet other) {
        set |= other.set;
    }

    public void and(PrivBitSet other) {
        set &= other.set;
    }

    public void xor(PrivBitSet other) {
        set ^= other.set;
    }

    public void remove(PrivBitSet privs) {
        PrivBitSet tmp = copy();
        tmp.xor(privs);
        and(tmp);
    }

    public boolean isEmpty() {
        return set == 0;
    }

    public boolean satisfy(PrivPredicate wantPrivs) {
        if (wantPrivs.getOp() == Operator.AND) {
            return (set & wantPrivs.getPrivs().set) == wantPrivs.getPrivs().set;
        } else {
            return (set & wantPrivs.getPrivs().set) != 0;
        }

    }
    
    public boolean containsNodeOrGrantPriv() {
        return containsPrivs(PaloPrivilege.NODE_PRIV, PaloPrivilege.GRANT_PRIV);
    }

    public boolean containsPrivs(PaloPrivilege... privs) {
        for (PaloPrivilege priv : privs) {
            if (get(priv.getIdx())) {
                return true;
            }
        }
        return false;
    }

    public List<PaloPrivilege> toPrivilegeList() {
        List<PaloPrivilege> privs = Lists.newArrayList();
        for (int i = 0; i < PaloPrivilege.privileges.length; i++) {
            if (get(i)) {
                privs.add(PaloPrivilege.getPriv(i));
            }
        }
        return privs;
    }

    public static PrivBitSet of(PaloPrivilege... privs) {
        PrivBitSet bitSet = new PrivBitSet();
        for (PaloPrivilege priv : privs) {
            bitSet.set(priv.getIdx());
        }
        return bitSet;
    }

    public static PrivBitSet of(List<PaloPrivilege> privs) {
        PrivBitSet bitSet = new PrivBitSet();
        for (PaloPrivilege priv : privs) {
            bitSet.set(priv.getIdx());
        }
        return bitSet;
    }

    public PrivBitSet copy() {
        PrivBitSet newSet = new PrivBitSet();
        newSet.set = set;
        return newSet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < PaloPrivilege.privileges.length; i++) {
            if (get(i)) {
                sb.append(PaloPrivilege.getPriv(i)).append(" ");
            }
        }
        return sb.toString();
    }

    public static PrivBitSet read(DataInput in) throws IOException {
        PrivBitSet privBitSet = new PrivBitSet();
        privBitSet.readFields(in);
        return privBitSet;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(set);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        set = in.readLong();
    }
}
