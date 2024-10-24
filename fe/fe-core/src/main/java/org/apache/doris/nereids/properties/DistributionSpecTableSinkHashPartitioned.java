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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.ExprId;

import java.util.List;

/**
 * use for shuffle data by partition keys before sink.
 */
public class DistributionSpecTableSinkHashPartitioned extends DistributionSpec {

    private List<ExprId> outputColExprIds;

    public DistributionSpecTableSinkHashPartitioned() {
        super();
    }

    public List<ExprId> getOutputColExprIds() {
        return outputColExprIds;
    }

    public void setOutputColExprIds(List<ExprId> outputColExprIds) {
        this.outputColExprIds = outputColExprIds;
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecTableSinkHashPartitioned;
    }
}
