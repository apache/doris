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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Strings;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// TODO(tsy): maybe better to rename as `LoadLabel`
// label name used to identify a load job
public class LabelName implements Writable {
    private String dbName;
    private String labelName;

    public LabelName() {

    }

    public LabelName(String dbName, String labelName) {
        this.dbName = dbName;
        this.labelName = labelName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLabelName() {
        return labelName;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            if (Strings.isNullOrEmpty(analyzer.getDefaultDb())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            dbName = analyzer.getDefaultDb();
        }
        FeNameFormat.checkLabel(labelName);
    }

    @Override
    public boolean equals(Object rhs) {
        if (this == rhs) {
            return true;
        }
        if (rhs instanceof LabelName) {
            LabelName rhsLabel = (LabelName) rhs;
            return this.dbName.equals(rhsLabel.dbName) && this.labelName.equals(rhsLabel.labelName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(dbName).append(labelName).toHashCode();
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("`").append(dbName).append("`.`").append(labelName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dbName);
        Text.writeString(out, labelName);
    }

    public void readFields(DataInput in) throws IOException {
        dbName = Text.readString(in);
        labelName = Text.readString(in);
    }
}
