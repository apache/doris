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

package org.apache.doris.udf;

import org.apache.doris.jni.spi.UdfExecutorFactory;

/**
 * Builds {@link UdfExecutor}, which runs both scalar functions and table functions. BE reaches it
 * as {@code (java-udf, scalar)} from the expression path and from the table-function path alike.
 */
public class ScalarUdfExecutorFactory implements UdfExecutorFactory {

    public static final String NAME = "scalar";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Object create(byte[] thriftParams) throws Exception {
        return new UdfExecutor(thriftParams);
    }

    /**
     * Both factories in this plugin share one cache of compiled functions, so this is the same
     * work {@link AggregateUdfExecutorFactory} does; BE broadcasts DROP FUNCTION to every factory
     * and dropping a signature that is not cached does nothing.
     */
    @Override
    public void invalidate(String functionSignature) {
        UdfClassCacheRegistry.invalidate(functionSignature);
    }
}
