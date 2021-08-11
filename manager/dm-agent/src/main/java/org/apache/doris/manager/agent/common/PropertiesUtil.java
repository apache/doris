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
package org.apache.doris.manager.agent.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertiesUtil {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtil.class);
    private static final String DEFAULT_PROPERTIES = "/application.properties";

    /**
     * get properties key
     *
     * @param propKey
     * @return
     */
    public static String getPropValue(String propKey) {
        try {
            Properties props = new Properties();
            InputStream inputStream = PropertiesUtil.class.getResourceAsStream(DEFAULT_PROPERTIES);
            BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            props.load(bf);
            return props.getProperty(propKey);
        } catch (IOException e) {
            log.error("get propKey error:", e);
        }
        return null;
    }
}
