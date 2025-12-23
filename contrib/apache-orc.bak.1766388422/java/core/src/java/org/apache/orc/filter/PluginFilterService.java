/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.filter;

import org.apache.hadoop.conf.Configuration;

/**
 * Service to determine Plugin filters to be used during read. The plugin filters determined are
 * combined using AND.
 * The filter is expected to be deterministic (for reattempts) and agnostic of the application order
 * which is non-deterministic.
 */
public interface PluginFilterService {
  /**
   * Determine the filter for a given read path. The determination is based on the path and the
   * read configuration, this should be carefully considered when using this in queries that might
   * refer to the same table/files with multiple aliases.
   *
   * @param filePath The fully qualified file path that is being read
   * @param config   The read configuration is supplied as input. This should not be changed.
   * @return The plugin filter determined for the given filePath
   */
  BatchFilter getFilter(String filePath, Configuration config);
}
