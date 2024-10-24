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

import org.apache.doris.mysql.privilege.Privilege;

// Same as defined in PrivPredicate.java
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
    public static DorisAccessType toAccessType(Privilege privilege) {
        switch (privilege) {
            case ADMIN_PRIV:
                return ADMIN;
            case NODE_PRIV:
                return NODE;
            case GRANT_PRIV:
                return GRANT;
            case SELECT_PRIV:
                return SELECT;
            case LOAD_PRIV:
                return LOAD;
            case ALTER_PRIV:
                return ALTER;
            case CREATE_PRIV:
                return CREATE;
            case DROP_PRIV:
                return DROP;
            case USAGE_PRIV:
                return USAGE;
            case SHOW_VIEW_PRIV:
                return SHOW_VIEW;
            default:
                return NONE;
        }
    }
}
