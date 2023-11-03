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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * rename
 */
public class AlterMTMVRenameInfo extends AlterMTMVInfo {
    private final String newName;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVRenameInfo(TableNameInfo mvName, String newName) {
        super(mvName);
        this.newName = Objects.requireNonNull(newName, "require newName object");
    }

    /**
     * analyze
     * @param ctx ctx
     * @throws AnalysisException AnalysisException
     */
    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        try {
            FeNameFormat.checkTableName(newName);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public void run() throws DdlException {
        throw new AnalysisException("current not support.");
    }
}
