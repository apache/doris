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
package org.apache.doris.demo.flink.elasticsearch;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * custom elasticsearch source
 */

public class ElasticsearchInput extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchInput.class);

    private final List<HttpHost> httpHosts;
    private final RestClientFactory restClientFactory;
    private final String index;

    private transient RestHighLevelClient client;

    private String scrollId;
    private SearchRequest searchRequest;

    private RowTypeInfo rowTypeInfo;

    private boolean hasNext;

    private Iterator<Map<String, Object>> iterator;
    private Map<String, Integer> position;


    public ElasticsearchInput(List<HttpHost> httpHosts,
                              RestClientFactory restClientFactory, String index) {
        this.httpHosts = httpHosts;
        this.restClientFactory = restClientFactory;
        this.index = index;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        search();
    }

    // get data from es
    protected void search() throws IOException{
        SearchResponse searchResponse;
        if(scrollId == null){
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
        }else{
            searchResponse = client.searchScroll(new SearchScrollRequest(scrollId),RequestOptions.DEFAULT);
        }

        if(searchResponse == null || searchResponse.getHits().getTotalHits().value < 1){
            hasNext = false;
            return;
        }

        hasNext = true;
        iterator =  Arrays.stream(searchResponse.getHits().getHits())
                .map(t -> t.getSourceAsMap())
                .collect(Collectors.toList()).iterator();
    }



    @Override
    public void openInputFormat() throws IOException {
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
        restClientFactory.configureRestClientBuilder(builder);
        client = new RestHighLevelClient(builder);

        position = new CaseInsensitiveMap();
        int i = 0;
        for(String name : rowTypeInfo.getFieldNames()){
            position.put(name, i++);
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //The test data is relatively small, so the commonly used ones here are relatively small
        searchSourceBuilder.size(50);

        searchSourceBuilder.fetchSource(rowTypeInfo.getFieldNames(), null);

        // Get data using scroll api
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(3l));

        searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.scroll(scroll);
        searchRequest.source(searchSourceBuilder);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        if(!hasNext) return null;

        if(!iterator.hasNext()){
            this.search();
            if(!hasNext || !iterator.hasNext()){
                hasNext = false;
                return null;
            }
        }

        for(Map.Entry<String, Object> entry: iterator.next().entrySet()){
            Integer p = position.get(entry.getKey());
            if(p == null) throw new IOException("unknown field "+entry.getKey());

            reuse.setField(p, entry.getValue());
        }

        return reuse;
    }

    @Override
    public void close() throws IOException {
        if(client == null)
            return;

        iterator = null;
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        try {
            client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT).isSucceeded();
            client.close();
            client = null;
        } catch (Exception exception) {
            if(!(exception.getMessage()).contains("Unable to parse response body")){
                throw exception;
            }
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowTypeInfo;
    }

    public static Builder builder(List<HttpHost> httpHosts, String index){
        return new Builder(httpHosts, index);
    }

    @PublicEvolving
    public static class Builder {
        private final List<HttpHost> httpHosts;
        private String index;
        private RowTypeInfo rowTypeInfo;
        private RestClientFactory restClientFactory = restClientBuilder -> {
        };

        public Builder(List<HttpHost> httpHosts, String index) {
            this.httpHosts = Preconditions.checkNotNull(httpHosts);
            this.index = index;
        }


        public Builder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            this.rowTypeInfo = rowTypeInfo;
            return this;
        }


        public ElasticsearchInput build() {
            Preconditions.checkNotNull(this.rowTypeInfo);
            ElasticsearchInput input =  new ElasticsearchInput(httpHosts,  restClientFactory, index);
            input.rowTypeInfo = this.rowTypeInfo;
            return input;
        }

    }
}
