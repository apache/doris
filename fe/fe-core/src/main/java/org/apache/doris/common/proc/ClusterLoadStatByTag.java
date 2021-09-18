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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

// SHOW PROC "/cluster_balance/cluster_load_stat"
public class ClusterLoadStatByTag implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add(
            "Tag").build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        Set<Tag> tags = genTagMap();
        for (Tag tag : tags) {
            result.addRow(Lists.newArrayList(tag.toKey()));
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        Set<Tag> tags = genTagMap();
        Map<String, Tag> tagMap = Maps.newHashMap();
        for (Tag tag : tags) {
            tagMap.put(tag.toKey(), tag);
        }
        Tag tag = tagMap.get(name);
        if (tag == null) {
            throw new AnalysisException("No such tag: " + name);
        }
        return new ClusterLoadStatByTagAndMedium(tag);
    }

    private Set<Tag> genTagMap() {
        Set<Tag> tags = Sets.newHashSet();
        List<Long> beIds = Catalog.getCurrentSystemInfo().getBackendIds(false);
        for (long beId : beIds) {
            Backend be = Catalog.getCurrentSystemInfo().getBackend(beId);
            if (be != null) {
                tags.add(be.getTag());
            }
        }
        return tags;
    }
}
