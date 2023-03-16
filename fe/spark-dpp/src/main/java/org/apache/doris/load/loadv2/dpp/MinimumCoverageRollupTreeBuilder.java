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

import org.apache.doris.sparkdpp.EtlJobConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Build RollupTree by using minimum coverage strategy,
// which is to find the index with the minimum columns that
// has all columns of rollup index as parent index node.
// Eg:
// There are three indexes:
//   index1(c1, c2, c3, c4, c5)
//   index2(c1, c2, c4)
//   index3(c1, c2)
//   index4(c3, c4)
//   index5(c1, c2, c5)
// then the result tree is:
//          index1
//      |     \      \
//  index2  index4   index5
//    |
//  index3
// Now, if there are more than one indexes meet the column coverage requirement,
// have the same column size(eg: index2 vs index5), child rollup is preferred
// builded from the front index(eg: index3 is the child of index2). This can be
// further optimized based on the row number of the index.
public class MinimumCoverageRollupTreeBuilder implements RollupTreeBuilder {
    public RollupTreeNode build(EtlJobConfig.EtlTable tableMeta) {
        List<EtlJobConfig.EtlIndex> indexes = tableMeta.indexes;
        List<EtlJobConfig.EtlIndex> indexMetas = new ArrayList<>();
        EtlJobConfig.EtlIndex baseIndex = null;
        for (EtlJobConfig.EtlIndex indexMeta : indexes) {
            if (indexMeta.isBaseIndex) {
                baseIndex = indexMeta;
                continue;
            }
            indexMetas.add(indexMeta);
        }
        List<EtlJobConfig.EtlColumn> baseIndexColumns = baseIndex.columns;
        List<String> baseKeyColumns = new ArrayList<>();
        List<String> baseValueColumns = new ArrayList<>();
        for (EtlJobConfig.EtlColumn columnMeta : baseIndexColumns) {
            if (columnMeta.isKey) {
                baseKeyColumns.add(columnMeta.columnName);
            } else {
                baseValueColumns.add(columnMeta.columnName);
            }
        }
        RollupTreeNode root = new RollupTreeNode();
        root.parent = null;
        root.keyColumnNames = baseKeyColumns;
        root.valueColumnNames = baseValueColumns;
        root.indexId = baseIndex.indexId;
        root.indexMeta = baseIndex;

        // sort the index metas to make sure the column number decrease
        Collections.sort(indexMetas, new EtlJobConfig.EtlIndexComparator().reversed());
        for (int i = 0; i < indexMetas.size(); ++i) {
            List<String> keyColumns = new ArrayList<>();
            List<String> valueColumns = new ArrayList<>();
            for (EtlJobConfig.EtlColumn column : indexMetas.get(i).columns) {
                if (column.isKey) {
                    keyColumns.add(column.columnName);
                } else {
                    valueColumns.add(column.columnName);
                }
            }
            if (!insertIndex(root, indexMetas.get(i), keyColumns, valueColumns)) {
                throw new RuntimeException(String.format("can't find a parent rollup for rollup %s,"
                                + " rollup tree is %s", indexMetas.get(i).toString(), root));
            }
        }
        return root;
    }

    // DFS traverse to build the rollup tree
    // return true means we find a parent rollup for current rollup table
    private boolean insertIndex(RollupTreeNode root, EtlJobConfig.EtlIndex indexMeta,
                             List<String> keyColumns,
                             List<String> valueColumns) {
        // find suitable parent rollup from current node's children
        if (root.children != null) {
            for (int i = root.children.size() - 1; i >= 0; i--) {
                if (insertIndex(root.children.get(i), indexMeta, keyColumns, valueColumns)) {
                    return true;
                }
            }
        }

        // find suitable parent rollup from current node
        if (root.keyColumnNames.containsAll(keyColumns) && root.valueColumnNames.containsAll(valueColumns)) {
            if (root.children == null) {
                root.children = new ArrayList<>();
            }
            RollupTreeNode newChild = new RollupTreeNode();
            newChild.keyColumnNames = keyColumns;
            newChild.valueColumnNames = valueColumns;
            newChild.indexMeta = indexMeta;
            newChild.indexId = indexMeta.indexId;
            newChild.parent = root;
            newChild.level = root.level + 1;
            root.children.add(newChild);
            return true;
        }

        return false;
    }
}
