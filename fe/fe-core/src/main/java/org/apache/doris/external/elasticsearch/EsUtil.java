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

import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.json.JSONObject;

public class EsUtil {
    
    public static void analyzePartitionAndDistributionDesc(PartitionDesc partitionDesc,
            DistributionDesc distributionDesc) throws AnalysisException {
        if (partitionDesc == null && distributionDesc == null) {
            return;
        }
        
        if (partitionDesc != null) {
            if (!(partitionDesc instanceof RangePartitionDesc)) {
                throw new AnalysisException("Elasticsearch table only permit range partition");
            }
            
            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            analyzePartitionDesc(rangePartitionDesc);
        }
        
        if (distributionDesc != null) {
            throw new AnalysisException("could not support distribution clause");
        }
    }
    
    private static void analyzePartitionDesc(RangePartitionDesc partDesc)
            throws AnalysisException {
        if (partDesc.getPartitionColNames() == null || partDesc.getPartitionColNames().isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }
        
        if (partDesc.getPartitionColNames().size() > 1) {
            throw new AnalysisException(
                    "Elasticsearch table's parition column could only be a single column");
        }
    }
    
    
    /**
     * get the json object from specified jsonObject
     *
     * @param jsonObject
     * @param key
     * @return
     */
    public static JSONObject getJsonObject(JSONObject jsonObject, String key, int fromIndex) {
        int firstOccr = key.indexOf('.', fromIndex);
        if (firstOccr == -1) {
            String token = key.substring(key.lastIndexOf('.') + 1);
            if (jsonObject.has(token)) {
                return jsonObject.getJSONObject(token);
            } else {
                return null;
            }
        }
        String fieldName = key.substring(fromIndex, firstOccr);
        if (jsonObject.has(fieldName)) {
            return getJsonObject(jsonObject.getJSONObject(fieldName), key, firstOccr + 1);
        } else {
            return null;
        }
    }
}
