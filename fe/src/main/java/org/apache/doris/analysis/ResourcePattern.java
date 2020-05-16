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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PaloAuth.PrivLevel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// only the following 2 formats are allowed
// *
// resource
public class ResourcePattern implements Writable {
    private String resourceName;
    boolean isAnalyzed = false;

    public static ResourcePattern ALL;
    static {
        ALL = new ResourcePattern("*");
        try {
            ALL.analyze();
        } catch (AnalysisException e) {
            // will not happen
        }
    }

    private ResourcePattern() {
    }

    public ResourcePattern(String resourceName) {
        this.resourceName = Strings.isNullOrEmpty(resourceName) ? "*" : resourceName;
    }

    public String getResourceName() {
        Preconditions.checkState(isAnalyzed);
        return resourceName;
    }

    public PrivLevel getPrivLevel() {
        Preconditions.checkState(isAnalyzed);
        if (resourceName.equals("*")) {
            return PrivLevel.GLOBAL;
        } else {
            return PrivLevel.RESOURCE;
        }
    }

    public void analyze() throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        if (!resourceName.equals("*")) {
            FeNameFormat.checkResourceName(resourceName);
        }
        isAnalyzed = true;
    }

    public static ResourcePattern read(DataInput in) throws IOException {
        ResourcePattern resourcePattern = new ResourcePattern();
        resourcePattern.readFields(in);
        return resourcePattern;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ResourcePattern)) {
            return false;
        }
        ResourcePattern other = (ResourcePattern) obj;
        return resourceName.equals(other.getResourceName());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + resourceName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return resourceName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkState(isAnalyzed);
        Text.writeString(out, resourceName);
    }

    public void readFields(DataInput in) throws IOException {
        resourceName = Text.readString(in);
        isAnalyzed = true;
    }
}
