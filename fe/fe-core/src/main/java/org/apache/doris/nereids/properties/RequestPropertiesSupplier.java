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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.RequestProperties.RequestPropertiesTree;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** RequestPropertiesSupplier */
public interface RequestPropertiesSupplier<P extends Plan> {
    List<Plan> children();

    RequestProperties getRequestProperties();

    Plan withRequestAndChildren(RequestProperties requestProperties, List<Plan> children);

    default P withRequest(RequestProperties requestProperties) {
        return (P) withRequestAndChildren(requestProperties, children());
    }

    /** withRequestsTree */
    default P withRequestTree(RequestPropertiesTree tree) {
        List<RequestPropertiesTree> childrenRequests = tree.children;
        List<Plan> children = children();
        if (!childrenRequests.isEmpty() && children.size() != childrenRequests.size()) {
            throw new AnalysisException("The number of RequestProperties mismatch the plan tree");
        }

        List<Plan> newChildren = children;
        if (!childrenRequests.isEmpty()) {
            ImmutableList.Builder<Plan> newChildrenBuilder =
                    ImmutableList.builderWithExpectedSize(childrenRequests.size());
            for (int i = 0; i < children.size(); i++) {
                Plan child = children.get(i);
                Preconditions.checkState(child instanceof RequestPropertiesSupplier,
                        "child should be RequestPropertiesTree: " + child);
                Plan newChild = ((RequestPropertiesSupplier<Plan>) child).withRequestTree(childrenRequests.get(i));
                newChildrenBuilder.add(newChild);
            }
            newChildren = newChildrenBuilder.build();
        }

        return (P) withRequestAndChildren(tree.requestProperties, newChildren);
    }
}
