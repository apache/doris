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

package org.apache.doris.nereids.trees;

/**
 * interface for all tree node that have three children.
 */
public interface TernaryNode<
            NODE_TYPE extends TreeNode<NODE_TYPE>,
            FIRST_CHILD_TYPE extends TreeNode,
            SECOND_CHILD_TYPE extends TreeNode,
            THIRD_CHILD_TYPE extends TreeNode>
        extends TreeNode<NODE_TYPE> {

    default FIRST_CHILD_TYPE first() {
        return (FIRST_CHILD_TYPE) child(0);
    }

    default SECOND_CHILD_TYPE second() {
        return (SECOND_CHILD_TYPE) child(1);
    }

    default THIRD_CHILD_TYPE third() {
        return (THIRD_CHILD_TYPE) child(2);
    }

    @Override
    default int arity() {
        return 3;
    }
}
