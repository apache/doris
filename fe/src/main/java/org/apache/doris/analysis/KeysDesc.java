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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class KeysDesc implements Writable {
    private KeysType type;
    private List<String> keysColumnNames;

    public KeysDesc() {
        this.type = KeysType.AGG_KEYS;
        this.keysColumnNames = Lists.newArrayList();
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames) {
        this.type = type;
        this.keysColumnNames = keysColumnNames;
    }

    public KeysType getKeysType() {
        return type;
    }

    public int keysColumnSize() {
        return keysColumnNames.size();
    }

    public boolean containsCol(String colName) {
        return keysColumnNames.contains(colName);
    }

    public void analyze(List<ColumnDef> cols) throws AnalysisException {
        if (type == null) {
            throw new AnalysisException("Keys type is null.");
        }

        if (keysColumnNames == null || keysColumnNames.size() == 0) {
            throw new AnalysisException("The number of key columns is 0.");
        }

        if (keysColumnNames.size() > cols.size()) {
            throw new AnalysisException("The number of key columns should be less than the number of columns.");
        }

        for (int i = 0; i < keysColumnNames.size(); ++i) {
            String name = cols.get(i).getName();
            if (!keysColumnNames.get(i).equalsIgnoreCase(name)) {
                throw new AnalysisException("Key columns should be a ordered prefix of the schema.");
            }

            if (cols.get(i).getAggregateType() != null) {
                throw new AnalysisException("Key column[" + name + "] should not specify aggregate type.");
            }
        }

        // for olap table
        for (int i = keysColumnNames.size(); i < cols.size(); ++i) {
            if (type == KeysType.AGG_KEYS) {
                if (cols.get(i).getAggregateType() == null) {
                    throw new AnalysisException(type.name() + " table should specify aggregate type for "
                            + "non-key column[" + cols.get(i).getName() + "]");
                }
            } else {
                if (cols.get(i).getAggregateType() != null) {
                    throw new AnalysisException(type.name() + " table should not specify aggregate type for "
                            + "non-key column[" + cols.get(i).getName() + "]");
                }
            }
        }
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.name()).append("(");
        int i = 0;
        for (String columnName : keysColumnNames) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(columnName).append("`");
            i++;
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    public static KeysDesc read(DataInput in) throws IOException {
        KeysDesc desc = new KeysDesc();
        desc.readFields(in);
        return desc;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        int count = keysColumnNames.size();
        out.writeInt(count);
        for (String colName : keysColumnNames) {
            Text.writeString(out, colName);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = KeysType.valueOf(Text.readString(in));

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            keysColumnNames.add(Text.readString(in));
        }
    }
}

