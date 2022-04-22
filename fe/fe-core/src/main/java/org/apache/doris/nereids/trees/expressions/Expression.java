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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.types.DataType;

/**
 * Abstract class for all Expression in Nereids.
 */
public abstract class Expression extends TreeNode<Expression> {
    public Expression(NodeType type) {
        super(type);
    }

    public DataType getDataType() throws UnboundException {
        throw new UnboundException("dataType");
    }

    public String sql() throws UnboundException {
        throw new UnboundException("sql");
    }

    public boolean nullable() throws UnboundException {
        throw new UnboundException("nullable");
    }
}
