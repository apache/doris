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

package org.apache.doris.load.loadv2;

import org.apache.spark.launcher.SparkAppHandle;

public class SparkPendingTaskAttachment extends TaskAttachment {
    private SparkAppHandle handle;
    private String appId;
    private String outputPath;

    public SparkPendingTaskAttachment(long taskId) {
        super(taskId);
    }

    public SparkAppHandle getHandle() {
        return handle;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public void setHandle(SparkAppHandle handle) {
        this.handle = handle;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public String toString() {
        return "SparkPendingTaskAttachment{" +
                "appId='" + appId + '\'' +
                ", outputPath='" + outputPath + '\'' +
                '}';
    }
}
