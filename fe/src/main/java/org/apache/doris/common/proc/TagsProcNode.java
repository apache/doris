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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.TagManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TagsProcNode implements ProcNodeInterface {
    private static final Logger LOG = LogManager.getLogger(TagsProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Tags").add("ResourceIds")
            .build();

    private TagManager tagManager;

    public TagsProcNode(TagManager tagManager) {
        this.tagManager = tagManager;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(tagManager);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(tagManager.getShowInfos());
        return result;
    }
}


