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

import org.apache.doris.mysql.privilege.PrivPredicate;

// Same as defined in PrivPredicate.java
public enum DorisAccessType {
    SHOW,
    SHOW_VIEW,
    SHOW_RESOURCES,
    SHOW_WORKLOAD_GROUP,
    GRANT,
    ADMIN,
    LOAD,
    ALTER,
    CREATE,
    ALTER_CREATE,
    ALTER_CREATE_DROP,
    DROP,
    SELECT,
    OPERATOR,
    USAGE,
    ALL,
    NODE,
    NONE;

    public static DorisAccessType toAccessType(PrivPredicate priv) {
        if (priv == PrivPredicate.SHOW) {
            return SHOW;
        } else if (priv == PrivPredicate.SHOW_VIEW) {
            return SHOW_VIEW;
        } else if (priv == PrivPredicate.SHOW_RESOURCES) {
            // For Ranger, there is only USAGE priv for RESOURCE and WORKLOAD_GROUP.
            // So when checking SHOW_XXX priv, convert it to USAGE priv and pass to Ranger.
            return USAGE;
        } else if (priv == PrivPredicate.SHOW_WORKLOAD_GROUP) {
            return USAGE;
        } else if (priv == PrivPredicate.GRANT) {
            return GRANT;
        } else if (priv == PrivPredicate.ADMIN) {
            return ADMIN;
        } else if (priv == PrivPredicate.LOAD) {
            return LOAD;
        } else if (priv == PrivPredicate.ALTER) {
            return ALTER;
        } else if (priv == PrivPredicate.CREATE) {
            return CREATE;
        } else if (priv == PrivPredicate.ALTER_CREATE) {
            return ALTER_CREATE;
        } else if (priv == PrivPredicate.ALTER_CREATE_DROP) {
            return ALTER_CREATE_DROP;
        } else if (priv == PrivPredicate.DROP) {
            return DROP;
        } else if (priv == PrivPredicate.SELECT) {
            return SELECT;
        } else if (priv == PrivPredicate.OPERATOR) {
            return OPERATOR;
        } else if (priv == PrivPredicate.USAGE) {
            return USAGE;
        } else if (priv == PrivPredicate.ALL) {
            return ALL;
        } else {
            return NONE;
        }
    }
}
