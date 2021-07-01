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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

/**
 * create a encryptKey
 *
 * The syntax is:
 * CREATE ENCRYPTKEY key_name
 *     AS "key_string";
 * `key_name`: The name of the key to be created, which can include the name of the database. For example: `db1.my_key`.
 * `key_string`: The string to create the key
 *
 * for example:
 *     CREATE ENCRYPTKEY test.key1 AS "beijing";
 */
public class CreateEncryptKeyStmt extends DdlStmt{
    private final EncryptKeyName encryptKeyName;
    private final String keyString;
    private EncryptKey encryptKey;

    public CreateEncryptKeyStmt(EncryptKeyName encryptKeyName, String keyString) {
        this.encryptKeyName = encryptKeyName;
        this.keyString = keyString;
    }

    public EncryptKeyName getEncryptKeyName() {
        return encryptKeyName;
    }

    public String getKeyString() {
        return keyString;
    }

    public EncryptKey getEncryptKey() {
        return encryptKey;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check operation privilege
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        encryptKeyName.analyze(analyzer);
        if (Strings.isNullOrEmpty(keyString)) {
            throw new AnalysisException("keyString can not be null or empty string.");
        }
        encryptKey = new EncryptKey(encryptKeyName, keyString);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE ENCRYPTKEY ")
                .append(encryptKeyName.getKeyName()).append(" AS \"").append(keyString).append("\"");
        return stringBuilder.toString();
    }
}
