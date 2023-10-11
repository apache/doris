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

import java.util.Optional;

public class PrivPredicate {

    // user can 'see' this meta
    public static final PrivPredicate SHOW = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.SELECT_PRIV,
            Privilege.LOAD_PRIV,
            Privilege.ALTER_PRIV,
            Privilege.CREATE_PRIV,
            Privilege.DROP_PRIV),
            Operator.OR);
    // show create table 'view'
    public static final PrivPredicate SHOW_VIEW = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.SHOW_VIEW_PRIV),
            Operator.OR);
    // show resources
    public static final PrivPredicate SHOW_RESOURCES = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.USAGE_PRIV),
            Operator.OR);
    public static final PrivPredicate SHOW_WORKLOAD_GROUP = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.USAGE_PRIV),
            Operator.OR);
    // create/drop/alter/show user
    public static final PrivPredicate GRANT = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.GRANT_PRIV),
            Operator.OR);
    // admin user privs
    public static final PrivPredicate ADMIN = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV),
            Operator.OR);

    // load
    public static final PrivPredicate LOAD = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.LOAD_PRIV),
            Operator.OR);

    // alter
    public static final PrivPredicate ALTER = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.ALTER_PRIV),
            Operator.OR);

    // create
    public static final PrivPredicate CREATE = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.CREATE_PRIV),
            Operator.OR);

    // drop
    public static final PrivPredicate DROP = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.DROP_PRIV),
            Operator.OR);

    // select
    public static final PrivPredicate SELECT = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.SELECT_PRIV),
            Operator.OR);

    // operator
    public static final PrivPredicate OPERATOR = PrivPredicate.of(PrivBitSet.of(Privilege.NODE_PRIV),
            Operator.OR);

    // resource/workloadGroup usage
    public static final PrivPredicate USAGE = PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
            Privilege.USAGE_PRIV),
            Operator.OR);

    // all
    public static final PrivPredicate ALL = PrivPredicate.of(PrivBitSet.of(Privilege.NODE_PRIV,
            Privilege.ADMIN_PRIV,
            Privilege.SELECT_PRIV,
            Privilege.LOAD_PRIV,
            Privilege.ALTER_PRIV,
            Privilege.CREATE_PRIV,
            Privilege.DROP_PRIV,
            Privilege.USAGE_PRIV),
            Operator.OR);

    private PrivBitSet privs;
    private Operator op;

    private PrivPredicate(PrivBitSet privs, Operator op) {
        this.privs = privs;
        this.op = op;
    }

    public static PrivPredicate of(PrivBitSet privs, Operator op) {
        final PrivPredicate predicate = new PrivPredicate(privs, op);
        return predicate;
    }

    public PrivBitSet getPrivs() {
        return privs;
    }

    public Operator getOp() {
        return op;
    }

    // Determine which column Privilege correspond to PrivPredicate
    //The current logic is to include a SELECT_ PRIV returns SELECT_ PRIV, if load is included_ PRIV returns LOAD_ PRIV,
    // the order cannot be reversed
    public Optional<Privilege> getColPrivilege() {
        if (privs.get(Privilege.SELECT_PRIV.getIdx())) {
            return Optional.of(Privilege.SELECT_PRIV);
        } else if (privs.get(Privilege.LOAD_PRIV.getIdx())) {
            return Optional.of(Privilege.LOAD_PRIV);
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("priv predicate: ").append(op).append(", ").append(privs);
        return sb.toString();
    }

}
