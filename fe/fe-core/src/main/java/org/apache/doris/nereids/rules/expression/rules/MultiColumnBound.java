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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;

/** MultiColumnBound */
public class MultiColumnBound implements Comparable<MultiColumnBound> {
    private final List<ColumnBound> columnBounds;

    public MultiColumnBound(List<ColumnBound> columnBounds) {
        this.columnBounds = Utils.fastToImmutableList(
                Objects.requireNonNull(columnBounds, "column bounds can not be null")
        );
    }

    @Override
    public int compareTo(MultiColumnBound o) {
        for (int i = 0; i < columnBounds.size(); i++) {
            ColumnBound columnBound = columnBounds.get(i);
            ColumnBound otherColumnBound = o.columnBounds.get(i);
            int result = columnBound.compareTo(otherColumnBound);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnBounds.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(columnBounds.get(i));
        }
        return sb.toString();
    }
}
