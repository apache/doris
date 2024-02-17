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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.EsTable;
import org.apache.doris.common.UserException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * It is responsible for this class to schedule all network request sent to remote ES Cluster
 * Request sequence
 * 1. GET {index}/_mapping
 * 2. GET {index}/_search_shards
 * <p>
 * note: step 1 is not necessary
 */
public class EsMetaStateTracker {

    private static final Logger LOG = LogManager.getLogger(EsMetaStateTracker.class);

    private List<SearchPhase> builtinSearchPhase = new LinkedList<>();
    private SearchContext searchContext;

    public EsMetaStateTracker(EsRestClient client, EsTable esTable) {
        builtinSearchPhase.add(new MappingPhase(client));
        builtinSearchPhase.add(new PartitionPhase(client));
        searchContext = new SearchContext(esTable);
    }

    public SearchContext searchContext() throws UserException {
        if (searchContext == null) {
            LOG.warn("ES meta state track failed, please check ES health status.");
            throw new UserException("ES meta state track failed, please check ES health status.");
        }
        return searchContext;
    }

    public void run() throws DorisEsException {
        for (SearchPhase searchPhase : builtinSearchPhase) {
            searchPhase.preProcess(searchContext);
            searchPhase.execute(searchContext);
            searchPhase.postProcess(searchContext);
        }
    }
}
