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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.ResourcePattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Collection;
import java.util.List;
import java.util.Set;

// ....0000000000
//        ^     ^
//        |     |
//        |     -- first priv(0)
//        |--------last priv(7)
public class PrivBitSet {

    @SerializedName(value = "set")
    private long set = 0;

    public PrivBitSet() {
    }

    public void set(int index) {
        Preconditions.checkState(Privilege.privileges.containsKey(index), index);
        set |= 1 << index;
    }

    public void unset(int index) {
        Preconditions.checkState(Privilege.privileges.containsKey(index), index);
        set &= ~(1 << index);
    }

    public boolean get(int index) {
        Preconditions.checkState(Privilege.privileges.containsKey(index), index);
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

    public void clean() {
        this.set = 0;
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

    public boolean containsNodePriv() {
        return containsPrivs(Privilege.NODE_PRIV);
    }

    public boolean containsResourcePriv() {
        return containsPrivs(Privilege.USAGE_PRIV, Privilege.CLUSTER_USAGE_PRIV, Privilege.STAGE_USAGE_PRIV);
    }

    public boolean containsDbTablePriv() {
        return containsPrivs(Privilege.SELECT_PRIV, Privilege.LOAD_PRIV, Privilege.ALTER_PRIV,
                Privilege.CREATE_PRIV, Privilege.DROP_PRIV);
    }

    public boolean containsPrivs(Privilege... privs) {
        for (Privilege priv : privs) {
            if (get(priv.getIdx())) {
                return true;
            }
        }
        return false;
    }

    public List<Privilege> toPrivilegeList() {
        List<Privilege> privs = Lists.newArrayList();
        Privilege.privileges.keySet().forEach(idx -> {
            if (get(idx)) {
                privs.add(Privilege.getPriv(idx));
            }
        });
        return privs;
    }

    public static PrivBitSet of(Privilege... privs) {
        PrivBitSet bitSet = new PrivBitSet();
        for (Privilege priv : privs) {
            bitSet.set(priv.getIdx());
        }
        return bitSet;
    }

    public static PrivBitSet of(Collection<Privilege> privs) {
        PrivBitSet bitSet = new PrivBitSet();
        for (Privilege priv : privs) {
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
        Privilege.privileges.keySet().forEach(idx -> {
            if (get(idx)) {
                sb.append(Privilege.getPriv(idx)).append(",");
            }
        });
        String res = sb.toString();
        if (res.length() > 0) {
            return res.substring(0, res.length() - 1);
        } else {
            return res;
        }
    }

    public static void convertResourcePrivToCloudPriv(ResourcePattern resourcePattern, Set<Privilege> privileges) {
        if (privileges.size() != 1 || !privileges.contains(Privilege.USAGE_PRIV)) {
            return;
        }
        switch (resourcePattern.getResourceType()) {
            case CLUSTER:
                privileges.clear();
                privileges.add(Privilege.CLUSTER_USAGE_PRIV);
                break;
            case STAGE:
                privileges.clear();
                privileges.add(Privilege.STAGE_USAGE_PRIV);
                break;
            default:
                break;
        }
    }
}
