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

package org.apache.doris.filesystem.properties;

import java.util.Map;

/**
 * Storage properties that can be exported as Hadoop-compatible configuration.
 *
 * <p>The API module returns a map instead of org.apache.hadoop.conf.Configuration
 * so provider metadata can stay independent from Hadoop dependencies. Provider
 * modules or callers that already depend on Hadoop can materialize the map into
 * a Configuration instance.</p>
 */
public interface HadoopStorageProperties {

    /**
     * Converts to a Hadoop configuration map without exposing Hadoop dependencies in
     * the API layer. Keys should use Hadoop configuration names such as fs.s3a.*.
     *
     * @return Hadoop configuration key-value pairs for this storage provider
     */
    Map<String, String> toHadoopConfigurationMap();
}
