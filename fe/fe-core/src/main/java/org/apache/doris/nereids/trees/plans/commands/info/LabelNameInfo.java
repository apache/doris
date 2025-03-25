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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TableName.java
// and modified by Doris

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.LabelName;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Objects;

/**
 * Label name info
 */
public class LabelNameInfo {
    private String label;
    private String db;

    public LabelNameInfo() {

    }

    /**
     * TableNameInfo
     * @param db dbName
     * @param label tblName
     */
    public LabelNameInfo(String db, String label) {
        Objects.requireNonNull(label, "require label object");
        this.label = label;
        if (Env.isStoredTableNamesLowerCase()) {
            this.label = label.toLowerCase();
        }
        this.db = db;
    }

    /**
     * validate labelNameInfo
     * @param ctx ctx
     */
    public void validate(ConnectContext ctx) throws org.apache.doris.common.AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = ctx.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                throw new AnalysisException("No database selected");
            }
        }

        if (Strings.isNullOrEmpty(label)) {
            throw new AnalysisException("Table name is null");
        }

        FeNameFormat.checkLabel(label);
    }

    /**
     * get db name
     * @return dbName
     */
    public String getDb() {
        return db;
    }

    /**
     * set a new database name
     * @param db new database name
     */
    public void setDb(String db) {
        this.db = db;
    }

    /**
     * get label name
     * @return labelName
     */
    public String getLabel() {
        return label;
    }

    /**
     * transferToLabelName
     * @return LabelName
     */
    public LabelName transferToLabelName() {
        return new LabelName(db, label);
    }
}
