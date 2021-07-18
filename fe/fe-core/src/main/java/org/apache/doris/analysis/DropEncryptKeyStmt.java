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

import org.apache.doris.catalog.EncryptKeySearchDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

public class DropEncryptKeyStmt extends DdlStmt {
    private final EncryptKeyName encryptKeyName;
    private EncryptKeySearchDesc encryptKeySearchDesc;

    public DropEncryptKeyStmt(EncryptKeyName encryptKeyName) {
        this.encryptKeyName = encryptKeyName;
    }

    public EncryptKeyName getEncryptKeyName() {
        return encryptKeyName;
    }

    public EncryptKeySearchDesc getEncryptKeysSearchDesc() {
        return encryptKeySearchDesc;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        // analyze encryptkey name
        encryptKeyName.analyze(analyzer);
        encryptKeySearchDesc = new EncryptKeySearchDesc(encryptKeyName);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP ENCRYPTKEY ").append(encryptKeyName.getKeyName());
        return stringBuilder.toString();
    }
}
