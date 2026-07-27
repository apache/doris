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

package org.apache.doris.alter;


public enum AlterUserOpType {
    SET_PASSWORD,
    SET_ROLE,
    SET_PASSWORD_POLICY,
    LOCK_ACCOUNT,
    UNLOCK_ACCOUNT,
    MODIFY_COMMENT,
    SET_TLS_REQUIRE,
    // MySQL-compatible "DISCARD OLD PASSWORD": evict the retained secondary
    // password. NB: NEVER journaled via OP_ALTER_USER — a pre-feature binary
    // would fail replay on the unknown enum value. It rides OP_SET_PASSWORD
    // instead (PrivInfo.discardPasswd; see Auth.discardOldPasswordInternal),
    // which older binaries replay as a harmless same-password set.
    DISCARD_OLD_PASSWORD
}
