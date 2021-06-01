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

import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class EncryptKeyRef extends Expr {
    private static final Logger LOG = LogManager.getLogger(EncryptKeyRef.class);
    private EncryptKeyName encryptKeyName;

    private EncryptKeyRef() {
        super();
    }

    public EncryptKeyRef(EncryptKeyName encryptKeyName) {
        super();
        this.encryptKeyName = encryptKeyName;
    }

    public LiteralExpr analyzeEncryptKey(Analyzer analyzer) throws AnalysisException {
        List<EncryptKey> encryptKeys = analyzer.getEncryptKeysFromDb(encryptKeyName.getDb());
        for (EncryptKey encryptKey : encryptKeys) {
            String keyName = encryptKey.getEncryptKeyName().getKeyName();
            if (encryptKeyName.getKeyName().equals(keyName)) {
                StringLiteral retLiteral = (StringLiteral) LiteralExpr.create(encryptKey.getKeyString(), Type.VARCHAR);
                retLiteral.setEncryptKey(true);
                retLiteral.setEncryptKeyName(encryptKeyName.toString());
                return retLiteral;
            }
        }
        throw new AnalysisException("Can not found encryptKey: " + encryptKeyName.toString());
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {

    }

    @Override
    protected String toSqlImpl() {
        return null;
    }

    @Override
    protected void toThrift(TExprNode msg) {

    }

    @Override
    public Expr clone() {
        return null;
    }
}
