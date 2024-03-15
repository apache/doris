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

package org.apache.doris.common;

import org.apache.doris.journal.bdbje.BDBToolOptions;

import com.google.common.base.Strings;

public class CommandLineOptions {

    private boolean isVersion;
    private String helperNode;
    private boolean runBdbTools;
    private BDBToolOptions bdbToolOpts = null;
    private ImageToolOptions imageToolOpts = null;

    public CommandLineOptions(boolean isVersion, String helperNode, BDBToolOptions bdbToolOptions,
            ImageToolOptions imageToolOpts) {
        this.isVersion = isVersion;
        this.helperNode = helperNode;
        this.bdbToolOpts = bdbToolOptions;
        this.imageToolOpts = imageToolOpts;
        if (this.bdbToolOpts != null) {
            runBdbTools = true;
        } else {
            runBdbTools = false;
        }
    }

    public boolean isVersion() {
        return isVersion;
    }

    public String getHelperNode() {
        return helperNode;
    }

    public boolean runBdbTools() {
        return runBdbTools;
    }

    public BDBToolOptions getBdbToolOpts() {
        return bdbToolOpts;
    }

    public boolean runImageTool() {
        return imageToolOpts != null;
    }

    public ImageToolOptions getImageToolOpts() {
        return imageToolOpts;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("print version: " + isVersion).append("\n");
        sb.append("helper node: " + helperNode).append("\n");
        sb.append("bdb tool options: \n(\n" + bdbToolOpts).append("\n)\n");
        sb.append("image tool options:  \n(\n" + imageToolOpts).append("\n)\n");
        return sb.toString();
    }

    public static class ImageToolOptions {
        private final String imagePath;
        private final String dumpPath;

        public ImageToolOptions(String imagePath, String dumpPath) {
            this.imagePath = imagePath;
            this.dumpPath = dumpPath;
        }

        public String getImagePath() {
            return imagePath;
        }

        public boolean dumpImage() {
            return !Strings.isNullOrEmpty(dumpPath);
        }

        public String getDumpPath() {
            return dumpPath;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("image path: " + imagePath).append("\n");
            sb.append("dump path: " + dumpPath).append("\n");
            return sb.toString();
        }
    }
}
