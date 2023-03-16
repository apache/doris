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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/*
 * SHOW PROC '/jobs/'
 */
public class JobsDbProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName")
            .build();

    private Env env;

    public JobsDbProcDir(Env env) {
        this.env = env;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbIdStr)) {
            throw new AnalysisException("Db id is null");
        }

        long dbId;
        try {
            dbId = Long.valueOf(dbIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }

        // dbId = -1 means need total result of all databases
        Database db = dbId == -1 ? null : env.getInternalCatalog().getDbOrAnalysisException(dbId);

        return new JobsProcDir(env, db);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(env);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);
        List<String> names = env.getInternalCatalog().getDbNames();
        if (names == null || names.isEmpty()) {
            return result;
        }

        for (String name : names) {
            env.getInternalCatalog().getDb(name)
                    .ifPresent(db -> result.addRow(Lists.newArrayList(String.valueOf(db.getId()), name)));
        }

        return result;
    }

}
