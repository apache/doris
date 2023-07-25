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

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.nereids.util.Utils;

import java.util.List;

/**
 * index definition
 */
public class IndexDefinition {
    private final String name;
    private final List<String> cols;
    private final boolean isUseBitmap;
    private final String comment;

    public IndexDefinition(String name, List<String> cols, boolean isUseBitmap, String comment) {
        this.name = name;
        this.cols = Utils.copyRequiredList(cols);
        this.isUseBitmap = isUseBitmap;
        this.comment = comment;
    }

    public void validate() {
    }

    public Index translateToCatalogStyle() {
        return new Index(Env.getCurrentEnv().getNextId(), name, cols, isUseBitmap ? IndexType.BITMAP : null, null,
                comment);
    }
}
