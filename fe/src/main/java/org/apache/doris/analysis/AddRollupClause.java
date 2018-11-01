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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// used to create one rollup
// syntax:
//      ALTER TABLE table_name
//          ADD ROLLUP rollup_name (column, ..) FROM base_rollup
public class AddRollupClause extends AlterClause {
    private String rollupName;
    private List<String> columnNames;
    private String baseRollupName;
    private List<String> dupKeys;
    private Map<String, String> properties;

    public AddRollupClause() {
        columnNames = Lists.newArrayList();
        properties = Maps.newHashMap();
    }

    public String getRollupName() {
        return rollupName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getDupKeys() {
        return dupKeys;
    }

    public String getBaseRollupName() {
        return baseRollupName;
    }

    public AddRollupClause(String rollupName, List<String> columnNames,
                           List<String> dupKeys, String baseRollupName,
                           Map<String, String> properties) {
        this.rollupName = rollupName;
        this.columnNames = columnNames;
        this.dupKeys = dupKeys;
        this.baseRollupName = baseRollupName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        FeNameFormat.checkTableName(rollupName);

        if (columnNames == null || columnNames.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }
        Set<String> colSet = Sets.newHashSet();
        for (String col : columnNames) {
            if (Strings.isNullOrEmpty(col)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, col);
            }
            if (!colSet.add(col)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col);
            }
        }
        baseRollupName = Strings.emptyToNull(baseRollupName);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ADD ROLLUP `").append(rollupName).append("` (");
        int idx = 0;
        for (String column : columnNames) {
            if (idx != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(column).append("`");
            idx++;
        }
        stringBuilder.append(")");
        if (baseRollupName != null) {
            stringBuilder.append(" FROM `").append(baseRollupName).append("`");
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String className = AddRollupClause.class.getCanonicalName();
        Text.writeString(out, className);

        Text.writeString(out, rollupName);

        int count = columnNames.size();
        out.writeInt(count);
        for (String colName : columnNames) {
            Text.writeString(out, colName);
        }

        if (baseRollupName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, baseRollupName);
        }

        if (properties == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = properties.size();
            out.writeInt(count);
            for (Map.Entry<String, String> prop : properties.entrySet()) {
                Text.writeString(out, prop.getKey());
                Text.writeString(out, prop.getValue());
            }
        }
    }

    public void readFields(java.io.DataInput in) throws IOException {
        rollupName = Text.readString(in);
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String colName = Text.readString(in);
            columnNames.add(colName);
        }

        if (in.readBoolean()) {
            baseRollupName = Text.readString(in);
        }

        if (in.readBoolean()) {
            count = in.readInt();
            for (int i = 0; i < count; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                properties.put(key, value);
            }
        }
    }
}
