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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * rollup definition
 */
public class RollupDefinition {
    private final String name;
    private final List<String> cols;
    private final List<String> dupKeys;
    private final Map<String, String> properties;

    public RollupDefinition(String name, List<String> cols, List<String> dupKeys, Map<String, String> properties) {
        this.name = name;
        this.cols = Utils.copyRequiredList(cols);
        this.dupKeys = Utils.copyRequiredList(dupKeys);
        this.properties = Maps.newHashMap(properties);
    }

    /**
     * check rollup validity
     */
    public void validate() throws AnalysisException {
        Set<String> colSet = Sets.newHashSet();
        for (String col : cols) {
            if (!colSet.add(col)) {
                throw new AnalysisException("rollup has duplicate column name " + col);
            }
        }
    }

    public AddRollupClause translateToCatalogStyle() {
        return new AddRollupClause(name, cols, dupKeys, name, properties);
    }

    public String getName() {
        return name;
    }

    public List<String> getCols() {
        return cols;
    }
}
