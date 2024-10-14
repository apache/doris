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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.thrift.TExprNode;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

public class EncryptKeyRef extends Expr {
    @SerializedName("ekn")
    private EncryptKeyName encryptKeyName;
    private EncryptKey encryptKey;

    private EncryptKeyRef() {
        // only for serde
    }

    public EncryptKeyRef(EncryptKeyName encryptKeyName) {
        super();
        this.encryptKeyName = encryptKeyName;
        this.type = Type.VARCHAR;
    }

    protected EncryptKeyRef(EncryptKeyRef other) {
        super(other);
        this.encryptKeyName = other.encryptKeyName;
        this.encryptKey = other.encryptKey;
        this.type = Type.VARCHAR;
    }

    public EncryptKey getEncryptKey() {
        return encryptKey;
    }

    private void analyzeEncryptKey(Analyzer analyzer) throws AnalysisException {
        String dbName = encryptKeyName.getDb();
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
        }
        if ("".equals(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        } else {
            Database database = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);

            EncryptKey encryptKey = database.getEncryptKey(encryptKeyName.getKeyName());
            if (encryptKey != null) {
                this.encryptKey = encryptKey;
            } else {
                throw new AnalysisException("Can not found encryptKey: " + encryptKeyName.toString());
            }
        }

    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // analyze encryptKey name
        encryptKeyName.analyze(analyzer);
        // analyze encryptKey
        analyzeEncryptKey(analyzer);
    }

    @Override
    protected String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(encryptKeyName.toSql());
        return sb.toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // no operation
    }

    @Override
    public Expr clone() {
        return new EncryptKeyRef(this);
    }
}
