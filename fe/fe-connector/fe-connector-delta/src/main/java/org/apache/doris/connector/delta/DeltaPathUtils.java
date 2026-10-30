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

package org.apache.doris.connector.delta;

import org.apache.hadoop.fs.Path;

import java.net.URI;

/** Converts connector location URIs to the path strings accepted by Delta Kernel. */
final class DeltaPathUtils {
    private DeltaPathUtils() {
    }

    static String toKernelPath(String location) {
        // Kernel resolves unescaped Hadoop paths. Passing file:/GMT%2B9 directly would look
        // for a literal GMT%2B9 directory. The URI constructor decodes exactly once while
        // preserving literal '+' and a directory such as "%2B" encoded as "%252B".
        return new Path(URI.create(location)).toString();
    }
}
