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

package org.apache.doris.common.util;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

public class InternalDatabaseUtil {

    public static void checkDatabase(String dbName, ConnectContext ctx) throws AnalysisException {
        Preconditions.checkNotNull(dbName, "require dbName object");
        if (!FeConstants.INTERNAL_DB_NAME.equals(dbName)) {
            return;
        }
        if (ctx == null || ctx.getCurrentUserIdentity() == null || !ctx.getCurrentUserIdentity().isRootUser()) {
            throw new AnalysisException("Not allowed to operate database: " + dbName);
        }
    }
}
