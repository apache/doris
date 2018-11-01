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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import com.google.common.base.Preconditions;

/*
 * Represents an HDFS URI in a SQL statement.
 */
public class HdfsURI {
    private final String location;

    // Set during analysis
    // dhc to do
    // private Path uriPath;
    private String uriPath;

    public HdfsURI(String location) {
        Preconditions.checkNotNull(location);
        this.location = location.trim();
    }

    public String getPath() {
        Preconditions.checkNotNull(uriPath);
        return uriPath;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (location.isEmpty()) {
            throw new AnalysisException("URI path cannot be empty.");
        }
        uriPath = new String(location);
        //dhc to do
    /*
    uriPath = new Path(location);
    if (!uriPath.isUriPathAbsolute()) {
      throw new AnalysisException("URI path must be absolute: " + uriPath);
    }
    try {
      FileSystem fs = uriPath.getFileSystem(FileSystemUtil.getConfiguration());
      if (!(fs instanceof DistributedFileSystem)) {
        throw new AnalysisException(String.format("URI location '%s' " +
            "must point to an HDFS file system.", uriPath));
      }
    } catch (IOException e) {
      throw new AnalysisException(e.getMessage(), e);
    }

    // Fully-qualify the path
    uriPath = FileSystemUtil.createFullyQualifiedPath(uriPath);
    PrivilegeRequest req = new PrivilegeRequest(
        new AuthorizeableURI(uriPath.toString()), privilege);
    analyzer.getCatalog().checkAccess(analyzer.getUser(), req);
    */
    }

    @Override
    public String toString() {
        // If uriPath is null (this HdfsURI has not been analyzed yet) just return the raw
        // location string the caller passed in.
        return uriPath == null ? location : uriPath.toString();
    }

    public String getLocation() {
        return location;
    }
}
