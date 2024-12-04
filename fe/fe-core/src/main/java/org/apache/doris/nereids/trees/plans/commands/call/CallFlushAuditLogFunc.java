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

package org.apache.doris.nereids.trees.plans.commands.call;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;

/**
 * call flush_audit_log()
 * This will flush audit log immediately to the audit_log table.
 * Mainly for test cases, so that we don't need to wait 60 sec to flush the audit log.
 */
public class CallFlushAuditLogFunc extends CallFunc {

    private UserIdentity user;

    private CallFlushAuditLogFunc(UserIdentity user) {
        this.user = user;
    }

    public static CallFunc create(UserIdentity user, List<Expression> args) {
        if (!args.isEmpty()) {
            throw new AnalysisException("FLUSH_AUDIT_LOG function requires no parameter");
        }
        return new CallFlushAuditLogFunc(user);
    }

    @Override
    public void run() {
        // check priv
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(user, PrivPredicate.ADMIN)) {
            throw new AnalysisException("Only admin can flush audit log");
        }
        // flush audit log
        Env.getCurrentEnv().getPluginMgr().flushAuditLog();
    }
}
