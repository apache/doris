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

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.es.EsUtil;

import mockit.Expectations;
import mockit.Mocked;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

/**
 * Test case for es.
 **/
public class EsTestCase {

    @Mocked
    protected EsExternalTable mockEsExternalTable;

    protected String loadJsonFromFile(String fileName)
            throws IOException, URISyntaxException {
        File file = new File(EsUtil.class.getClassLoader()
                .getResource(fileName).toURI());
        InputStream is = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder jsonStr = new StringBuilder();
        String line = "";
        while ((line = br.readLine()) != null) {
            jsonStr.append(line);
        }
        br.close();
        is.close();
        return jsonStr.toString();
    }

    protected EsExternalTable fakeEsTable(String table, String index,
            String type, List<Column> columns) {
        new Expectations(mockEsExternalTable) {
            {
                mockEsExternalTable.getIndexName();
                result = index;
                minTimes = 0;

                mockEsExternalTable.getMappingType();
                result = type;
                minTimes = 0;

                mockEsExternalTable.isEnableDocValueScan();
                result = true;
                minTimes = 0;

                mockEsExternalTable.isEnableKeywordSniff();
                result = true;
                minTimes = 0;

                mockEsExternalTable.getMaxDocValueFields();
                result = 20;
                minTimes = 0;

                mockEsExternalTable.getColumn2typeMap();
                result = new HashMap<>();
                minTimes = 0;

                mockEsExternalTable.getFullSchema();
                result = columns;
                minTimes = 0;

                mockEsExternalTable.isNodesDiscovery();
                result = true;
                minTimes = 0;

                mockEsExternalTable.getName();
                result = table;
                minTimes = 0;
            }
        };
        return mockEsExternalTable;
    }
}
