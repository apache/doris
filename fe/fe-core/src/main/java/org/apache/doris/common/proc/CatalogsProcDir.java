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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.datasource.DataSourceIf;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /catalogs/
 * show all catalogs' info
 */
public class CatalogsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("CatalogIds").add("CatalogName").add("DatabaseNum")
            .build();

    private Env env;

    public CatalogsProcDir(Env env) {
        this.env = env;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String catalogIdStr) throws AnalysisException {
        if (env == null || Strings.isNullOrEmpty(catalogIdStr)) {
            throw new AnalysisException("Catalog id is null");
        }

        long catalogId = -1L;
        try {
            catalogId = Long.valueOf(catalogIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid catalog id format: " + catalogIdStr);
        }

        DataSourceIf ds = env.getDataSourceMgr().getCatalog(catalogId);
        if (ds == null) {
            throw new AnalysisException("Catalog " + catalogIdStr + " does not exist");
        }

        return new DbsProcDir(env, ds);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(env);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<Long> catalogIds = env.getDataSourceMgr().getCatalogIds();
        // get info
        List<List<Comparable>> catalogInfos = Lists.newArrayList();
        for (long catalogId : catalogIds) {
            DataSourceIf ds = env.getDataSourceMgr().getCatalog(catalogId);
            if (ds == null) {
                continue;
            }
            List<Comparable> catalogInfo = Lists.newArrayList();
            catalogInfo.add(ds.getId());
            catalogInfo.add(ds.getName());
            catalogInfo.add(ds.getDbNames().size());
            catalogInfos.add(catalogInfo);
        }

        // order by catalogId, asc
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(catalogInfos, comparator);

        // set result
        for (List<Comparable> info : catalogInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }
        return result;
    }
}
