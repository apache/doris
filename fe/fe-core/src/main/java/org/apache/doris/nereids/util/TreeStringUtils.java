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

package org.apache.doris.nereids.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Get tree like string describing query plan or Group.
 */
public class TreeStringUtils {

    public static String treeString(Object object, Function<Object, String> objectToString,
            Function<Object, List<Object>> childSupplier,
            Function<Object, List<Object>> extraPlansSupplier,
            Function<Object, Boolean> displaySupplier) {
        List<String> lines = new ArrayList<>();
        treeString(lines, new ArrayList<>(), object,
                objectToString, childSupplier, extraPlansSupplier, displaySupplier, false);
        return StringUtils.join(lines, "\n");
    }

    private static void treeString(List<String> lines, List<Boolean> lastChildren, Object object,
            Function<Object, String> objectToString, Function<Object, List<Object>> childrenSupplier,
            Function<Object, List<Object>> extraPlansSupplier,
            Function<Object, Boolean> displaySupplier,
            boolean isExtraPlan) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < lastChildren.size() - 1; i++) {
            sb.append(lastChildren.get(i) ? "   " : "|  ");
        }

        if (lastChildren.size() > 0) {
            Boolean last = lastChildren.get(lastChildren.size() - 1);
            sb.append(last ? "+-" : "|-");
            sb.append(isExtraPlan ? "*" : "-");
        }

        sb.append(objectToString.apply(object));
        lines.add(sb.toString());

        List<Object> allObjects = new ArrayList<>();
        List<Object> children = childrenSupplier.apply(object);
        List<Object> extraPlans = extraPlansSupplier.apply(object);
        boolean displayExtraPlanFirst = displaySupplier.apply(object);
        if (displayExtraPlanFirst) {
            allObjects.addAll(extraPlans);
            allObjects.addAll(children);
        } else {
            allObjects.addAll(children);
            allObjects.addAll(extraPlans);
        }
        for (int i = 0; i < allObjects.size(); i++) {
            List<Boolean> newLasts = new ArrayList<>(lastChildren);
            newLasts.add(i + 1 == allObjects.size());
            boolean isSubExtraPlan = displayExtraPlanFirst ? i < extraPlans.size() : i >= children.size();
            treeString(lines, newLasts, allObjects.get(i),
                    objectToString, childrenSupplier, extraPlansSupplier, displaySupplier, isSubExtraPlan);
        }
    }
}
