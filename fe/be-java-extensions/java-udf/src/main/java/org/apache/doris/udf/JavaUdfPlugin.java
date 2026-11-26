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

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.UdfExecutorFactory;

import java.util.Arrays;

/**
 * Entry point of the {@code java-udf} plugin, found through
 * {@code META-INF/services/org.apache.doris.jni.spi.DorisPlugin}.
 *
 * <p>It provides two executor factories, not three: a table function runs the same per-row
 * {@code evaluate} as a scalar function - the serialized parameters carry the flag that makes its
 * return type an array - so both are built by {@code scalar}. Aggregate functions have a different
 * method shape and get their own.
 */
public class JavaUdfPlugin implements DorisPlugin {

    @Override
    public Iterable<UdfExecutorFactory> getUdfExecutorFactories() {
        return Arrays.asList(new ScalarUdfExecutorFactory(), new AggregateUdfExecutorFactory());
    }
}
