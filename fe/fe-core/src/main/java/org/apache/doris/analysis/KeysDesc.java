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
import org.apache.doris.common.Config;
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
    private List<String> clusterKeysColumnNames;
    private List<Integer> clusterKeysColumnIds = null;

    public KeysDesc() {
        this.type = KeysType.AGG_KEYS;
        this.keysColumnNames = Lists.newArrayList();
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames) {
        this.type = type;
        this.keysColumnNames = keysColumnNames;
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames, List<String> clusterKeyColumnNames) {
        this(type, keysColumnNames);
        this.clusterKeysColumnNames = clusterKeyColumnNames;
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames, List<String> clusterKeyColumnNames,
                    List<Integer> clusterKeysColumnIds) {
        this(type, keysColumnNames, clusterKeyColumnNames);
        this.clusterKeysColumnIds = clusterKeysColumnIds;
    }

    public KeysType getKeysType() {
        return type;
    }

    public int keysColumnSize() {
        return keysColumnNames.size();
    }

    public List<String> getClusterKeysColumnNames() {
        return clusterKeysColumnNames;
    }

    public List<Integer> getClusterKeysColumnIds() {
        return clusterKeysColumnIds;
    }

    public boolean containsCol(String colName) {
        return keysColumnNames.contains(colName);
    }

    public void analyze(List<ColumnDef> cols) throws AnalysisException {
        if (type == null) {
            throw new AnalysisException("Keys type is null.");
        }

        if ((keysColumnNames == null || keysColumnNames.size() == 0) && type != KeysType.DUP_KEYS) {
            throw new AnalysisException("The number of key columns is 0.");
        }

        if (keysColumnNames.size() > cols.size()) {
            throw new AnalysisException("The number of key columns should be less than the number of columns.");
        }

        if (clusterKeysColumnNames != null) {
            if (Config.isCloudMode()) {
                throw new AnalysisException("Cluster key is not supported in cloud mode");
            }
            if (type != KeysType.UNIQUE_KEYS) {
                throw new AnalysisException("Cluster keys only support unique keys table.");
            }
            clusterKeysColumnIds = Lists.newArrayList();
            analyzeClusterKeys(cols);
        }

        for (int i = 0; i < keysColumnNames.size(); ++i) {
            String name = cols.get(i).getName();
            if (!keysColumnNames.get(i).equalsIgnoreCase(name)) {
                String keyName = keysColumnNames.get(i);
                if (cols.stream().noneMatch(col -> col.getName().equalsIgnoreCase(keyName))) {
                    throw new AnalysisException("Key column[" + keyName + "] doesn't exist.");
                }
                throw new AnalysisException("Key columns should be a ordered prefix of the schema."
                        + " KeyColumns[" + i + "] (starts from zero) is " + keyName + ", "
                        + "but corresponding column is " + name  + " in the previous "
                        + "columns declaration.");
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

        if (clusterKeysColumnNames != null) {
            int minKeySize = keysColumnNames.size() < clusterKeysColumnNames.size() ? keysColumnNames.size()
                    : clusterKeysColumnNames.size();
            boolean sameKey = true;
            for (int i = 0; i < minKeySize; ++i) {
                if (!keysColumnNames.get(i).equalsIgnoreCase(clusterKeysColumnNames.get(i))) {
                    sameKey = false;
                    break;
                }
            }
            if (sameKey) {
                throw new AnalysisException("Unique keys and cluster keys should be different.");
            }
        }
    }

    private void analyzeClusterKeys(List<ColumnDef> cols) throws AnalysisException {
        for (int i = 0; i < clusterKeysColumnNames.size(); ++i) {
            String name = clusterKeysColumnNames.get(i);
            // check if key is duplicate
            for (int j = 0; j < i; j++) {
                if (clusterKeysColumnNames.get(j).equalsIgnoreCase(name)) {
                    throw new AnalysisException("Duplicate cluster key column[" + name + "].");
                }
            }
            // check if key exists and generate key column ids
            for (int j = 0; j < cols.size(); j++) {
                if (cols.get(j).getName().equalsIgnoreCase(name)) {
                    cols.get(j).setClusterKeyId(clusterKeysColumnIds.size());
                    clusterKeysColumnIds.add(j);
                    break;
                }
                if (j == cols.size() - 1) {
                    throw new AnalysisException("Key cluster column[" + name + "] doesn't exist.");
                }
            }
        }
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.toSql()).append("(");
        int i = 0;
        for (String columnName : keysColumnNames) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(columnName).append("`");
            i++;
        }
        stringBuilder.append(")");
        if (clusterKeysColumnNames != null) {
            stringBuilder.append("\nCLUSTER BY (");
            i = 0;
            for (String columnName : clusterKeysColumnNames) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append("`").append(columnName).append("`");
                i++;
            }
            stringBuilder.append(")");
        }
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
        if (clusterKeysColumnNames == null) {
            out.writeInt(0);
        } else {
            out.writeInt(clusterKeysColumnNames.size());
            for (String colName : clusterKeysColumnNames) {
                Text.writeString(out, colName);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        type = KeysType.valueOf(Text.readString(in));

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            keysColumnNames.add(Text.readString(in));
        }
        count = in.readInt();
        if (count > 0) {
            clusterKeysColumnNames = Lists.newArrayList();
            for (int i = 0; i < count; i++) {
                clusterKeysColumnNames.add(Text.readString(in));
            }
        }
    }
}
