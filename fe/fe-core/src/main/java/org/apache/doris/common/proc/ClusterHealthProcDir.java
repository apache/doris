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

package org.apache.doris.common.proc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;

import java.util.ArrayList;
import java.util.List;

/*
 * show proc "/cluster_health";
 */

public class ClusterHealthProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Items").build();

    public ClusterHealthProcDir() {
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String itemStr) {
        if (itemStr.equalsIgnoreCase("tablet_health")) {
            return new TabletHealthProcDir(Catalog.getCurrentCatalog());
        }
        return null;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<List<String>> rows = new ArrayList<>(1);
        rows.add(Lists.newArrayList("tablet_health"));
        return new BaseProcResult(TITLE_NAMES, rows);
    }
}
