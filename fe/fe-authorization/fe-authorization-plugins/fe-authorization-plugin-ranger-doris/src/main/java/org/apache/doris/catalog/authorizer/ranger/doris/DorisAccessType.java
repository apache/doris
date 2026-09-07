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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.authorization.AccessAction;

// The access types a Doris Ranger service defines, one per privilege Doris grants.
public enum DorisAccessType {
    NODE,
    ADMIN,
    GRANT,
    SELECT,
    LOAD,
    ALTER,
    CREATE,
    DROP,
    USAGE,
    SHOW_VIEW,
    NONE;

    /**
     * The access type standing for {@code action}. The three ways of using something are one access type
     * here, which is a folding the Ranger service can afford and the engine cannot: its policies are written
     * against the resource, so a policy on a compute group and one on a stage are told apart by what they are
     * on rather than by the name of the privilege.
     */
    public static DorisAccessType of(AccessAction action) {
        switch (action) {
            case ADMIN:
                return ADMIN;
            case NODE:
                return NODE;
            case GRANT:
                return GRANT;
            case SELECT:
                return SELECT;
            case LOAD:
                return LOAD;
            case ALTER:
                return ALTER;
            case CREATE:
                return CREATE;
            case DROP:
                return DROP;
            case USAGE:
            case STAGE_USAGE:
            case CLUSTER_USAGE:
                return USAGE;
            case SHOW_VIEW:
                return SHOW_VIEW;
            default:
                // Guessing would ask Ranger about an access type its service definition does not have, and
                // every such request is denied - an action added to Doris would silently stop being grantable.
                throw new IllegalStateException("no Ranger access type for action " + action);
        }
    }
}
