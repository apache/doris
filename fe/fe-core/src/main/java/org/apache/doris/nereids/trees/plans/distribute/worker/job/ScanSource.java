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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.planner.ScanNode;

import java.util.List;

/** ScanSource */
public abstract class ScanSource {

    public abstract int maxParallel(List<ScanNode> scanNodes);

    public abstract List<ScanSource> parallelize(List<ScanNode> scanNodes, int instanceNum);

    public abstract boolean isEmpty();

    public abstract ScanSource newEmpty();

    public abstract void toString(StringBuilder str, String prefix);

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        toString(str, "");
        return str.toString();
    }
}
