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


package org.apache.doris.demo.spring.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicDataSourceContextHolder {

    public static final Logger log = LoggerFactory.getLogger(DynamicDataSourceContextHolder.class);

    /**
     * Use ThreadLocal to maintain variables. ThreadLocal provides an independent copy of the variable for each thread that uses the variable.
     * Therefore, each thread can independently change its own copy without affecting the copies corresponding to other threads.
     */
    private static final ThreadLocal<String> CONTEXT_HOLDER = new ThreadLocal<>();

    /**
     * Set the variables of the data source
     */
    public static void setDataSourceType(String dsType) {
        log.info("Switch to {} data source", dsType);
        CONTEXT_HOLDER.set(dsType);
    }

    /**
     * Get the variables of the data source
     */
    public static String getDataSourceType() {
        return CONTEXT_HOLDER.get();
    }

    /**
     * Clear data source variables
     */
    public static void clearDataSourceType() {
        CONTEXT_HOLDER.remove();
    }
}
