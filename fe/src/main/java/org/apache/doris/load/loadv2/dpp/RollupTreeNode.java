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

import org.apache.doris.load.loadv2.etl.EtlJobConfig;

import java.util.List;

// Base and rollup indexes are managed by as a RollupTree in order to
// produce the rollup index data from the best-fit index to get better performance.
// The calculation will be done through preorder traversal
public class RollupTreeNode {
    public RollupTreeNode parent;
    public List<RollupTreeNode> children;
    public long indexId;
    public List<String> keyColumnNames;
    public List<String> valueColumnNames;
    public int level;
    public EtlJobConfig.EtlIndex indexMeta;

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < level; ++i) {
            builder.append("-");
        }
        builder.append("indexid: " + indexId + "\n");
        if (children != null && !children.isEmpty()) {
            for (int i = 0; i < level; ++i) {
                builder.append("-");
            }
            builder.append("children:\n");
            for (RollupTreeNode child : children) {
                builder.append(child.toString());
            }
        }
        return builder.toString();
    }
}