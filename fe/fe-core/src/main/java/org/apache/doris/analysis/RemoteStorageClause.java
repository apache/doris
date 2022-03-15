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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Strings;

/**
 * Remote storage clause, including add/drop
 */
public class RemoteStorageClause extends AlterClause {
    // remote storage name
    private String storageName;

    public String getStorageName() {
        return storageName;
    }

    public RemoteStorageClause(String storageName) {
        super(AlterOpType.ALTER_OTHER);
        this.storageName = storageName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (Strings.isNullOrEmpty(storageName)) {
            throw new AnalysisException("Remote storage name cannot be empty.");
        }
    }

    @Override
    public String toSql() {
        throw new NotImplementedException();
    }
}
