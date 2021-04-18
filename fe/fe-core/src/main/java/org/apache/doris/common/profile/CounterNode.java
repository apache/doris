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

package org.apache.doris.common.profile;

import org.apache.doris.common.Pair;
import org.apache.doris.common.TreeNode;

public class CounterNode extends TreeNode<CounterNode> {
    private Pair<String, String> counter;

    public void setCounter(String key, String value) {
        counter = Pair.create(key, value);
    }

    public String toTree(int indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(debugString(indent));
        for (CounterNode node : getChildren()) {
            sb.append("\n").append(node.debugString(indent + 4));
        }
        return sb.toString();
    }

    public String debugString(int indent) {
        if (counter == null) {
            return printIndent(indent) + " - Counters:";
        }
        return printIndent(indent) + " - " + counter.first + ": " + counter.second;
    }

    private String printIndent(int indent) {
        String res = "";
        for (int i = 0; i < indent; i++) {
            res += " ";
        }
        return res;
    }
}
