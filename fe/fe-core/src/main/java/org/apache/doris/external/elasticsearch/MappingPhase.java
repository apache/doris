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

package org.apache.doris.external.elasticsearch;

/**
 * Get index mapping from remote ES Cluster, and resolved `keyword` and `doc_values` field
 * Later we can use it to parse all relevant indexes
 */
public class MappingPhase implements SearchPhase {

    private EsRestClient client;

    // json response for `{index}/_mapping` API
    private String jsonMapping;

    public MappingPhase(EsRestClient client) {
        this.client = client;
    }

    @Override
    public void execute(SearchContext context) throws DorisEsException {
        jsonMapping = client.getMapping(context.sourceIndex());
    }

    @Override
    public void postProcess(SearchContext context) {
        EsUtil.resolveFields(context, jsonMapping);
    }

}
