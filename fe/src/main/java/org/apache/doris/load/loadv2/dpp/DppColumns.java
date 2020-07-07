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

package org.apache.doris.load.loadv2.dpp;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.Comparator;

// DppColumns is used to store the
class DppColumns implements Comparable<DppColumns>, Serializable {
    public List<Object> columns = new ArrayList<Object>();;

    public DppColumns(List<Object> keys){
        this.columns = keys;
    }

    public DppColumns(DppColumns key, List<Integer> indexes){
        for (int i = 0; i < indexes.size(); ++i) {
            columns.add(key.columns.get(indexes.get(i)));
        }
    }

    @Override
    public int compareTo(DppColumns other) {
        Preconditions.checkState(columns.size() == other.columns.size());

        int cmp = 0;
        for (int i = 0; i < columns.size(); i++) {
            Object columnObj = columns.get(i);
            Object otherColumn = other.columns.get(i);
            if (columnObj == null && otherColumn == null) {
                return 0;
            } else if (columnObj == null || otherColumn == null) {
                if (columnObj == null) {
                    return -1;
                } else {
                    return 1;
                }
            }
            if (columns.get(i) instanceof Integer) {
                cmp = ((Integer)(columns.get(i))).compareTo((Integer)(other.columns.get(i)));
            } else if (columns.get(i) instanceof Long) {
                cmp = ((Long)(columns.get(i))).compareTo((Long)(other.columns.get(i)));
            }  else if (columns.get(i) instanceof  Boolean) {
                cmp = ((Boolean)(columns.get(i))).compareTo((Boolean) (other.columns.get(i)));
            } else if (columns.get(i) instanceof  Short) {
                cmp = ((Short)(columns.get(i))).compareTo((Short)(other.columns.get(i)));
            } else if (columns.get(i) instanceof  Float) {
                cmp = ((Float)(columns.get(i))).compareTo((Float) (other.columns.get(i)));
            } else if (columns.get(i) instanceof Double) {
                cmp = ((Double)(columns.get(i))).compareTo((Double) (other.columns.get(i)));
            } else if (columns.get(i) instanceof Date) {
                cmp = ((Date)(columns.get(i))).compareTo((Date) (other.columns.get(i)));
            } else if (columns.get(i) instanceof java.sql.Timestamp) {
                cmp = ((java.sql.Timestamp)columns.get(i)).compareTo((java.sql.Timestamp)other.columns.get(i));
            } else {
                cmp = ((String)(columns.get(i))).compareTo((String) (other.columns.get(i)));
            }
            if (cmp != 0) {
                return cmp;
            }
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DppColumns dppColumns = (DppColumns) o;
        return Objects.equals(columns, dppColumns.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return "dppColumns{" +
                "columns=" + columns +
                '}';
    }
}

class DppColumnsComparator implements Comparator<DppColumns> {
    @Override
    public int compare(DppColumns left, DppColumns right) {
        return left.compareTo(right);
    }
}