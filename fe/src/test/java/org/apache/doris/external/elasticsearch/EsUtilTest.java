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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class EsUtilTest {

    private String jsonStr = "{\"settings\": {\n" 
            + "               \"index\": {\n" 
            + "                  \"bpack\": {\n" 
            + "                     \"partition\": {\n" 
            + "                        \"upperbound\": \"12\"\n" 
            + "                     }\n" 
            + "                  },\n" 
            + "                  \"number_of_shards\": \"5\",\n" 
            + "                  \"provided_name\": \"indexa\",\n" 
            + "                  \"creation_date\": \"1539328532060\",\n" 
            + "                  \"number_of_replicas\": \"1\",\n" 
            + "                  \"uuid\": \"plNNtKiiQ9-n6NpNskFzhQ\",\n" 
            + "                  \"version\": {\n" 
            + "                     \"created\": \"5050099\"\n" 
            + "                  }\n" 
            + "               }\n" 
            + "            }}";
    
    @Test
    public void testGetJsonObject() {
        JSONObject json = new JSONObject(jsonStr);
        JSONObject upperBoundSetting = EsUtil.getJsonObject(json, "settings.index.bpack.partition", 0);
        assertTrue(upperBoundSetting.has("upperbound"));
        assertEquals("12", upperBoundSetting.getString("upperbound"));
        
        JSONObject unExistKey = EsUtil.getJsonObject(json, "set", 0);
        assertNull(unExistKey);
        
        JSONObject singleKey = EsUtil.getJsonObject(json, "settings", 0);
        assertTrue(singleKey.has("index"));
    }
    
    @Test(expected = JSONException.class)
    public void testGetJsonObjectWithException() {
        JSONObject json = new JSONObject(jsonStr);
        // only support json object could not get string value directly from this api, exception will be threw
        EsUtil.getJsonObject(json, "settings.index.bpack.partition.upperbound", 0);
    }

}
