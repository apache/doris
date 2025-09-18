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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;

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

    @Override
    protected String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(encryptKeyName.toSql());
        return sb.toString();
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
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
