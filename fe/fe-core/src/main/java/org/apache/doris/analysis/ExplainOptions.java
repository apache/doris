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

import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;

public class ExplainOptions {

    private boolean isVerbose;

    private boolean isTree;
    private boolean isGraph;

    private ExplainCommand.ExplainLevel explainLevel;

    public ExplainOptions(ExplainCommand.ExplainLevel explainLevel) {
        this.explainLevel = explainLevel;
    }

    public ExplainOptions(boolean isVerbose, boolean isTree, boolean isGraph) {
        this.isVerbose = isVerbose;
        this.isTree = isTree;
        this.isGraph = isGraph;
    }

    public boolean isVerbose() {
        return explainLevel == ExplainLevel.VERBOSE || isVerbose;
    }

    public boolean isTree() {
        return explainLevel == ExplainLevel.TREE || isTree;
    }

    public boolean isGraph() {
        return explainLevel == ExplainLevel.GRAPH || isGraph;
    }

    public boolean hasExplainLevel() {
        return explainLevel != null;
    }

    public ExplainLevel getExplainLevel() {
        return explainLevel;
    }
}
