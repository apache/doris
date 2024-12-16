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

package org.apache.doris.mysql.privilege;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.IOException;

public class ResourcePrivEntry extends PrivEntry {
    protected PatternMatcher resourcePattern;
    protected String origResource;

    protected ResourcePrivEntry() {
    }

    protected ResourcePrivEntry(PatternMatcher resourcePattern,
            String origResource, PrivBitSet privSet) {
        super(privSet);
        this.resourcePattern = resourcePattern;
        this.origResource = origResource;
    }

    public static ResourcePrivEntry create(String resourceName, PrivBitSet privs)
            throws AnalysisException, PatternMatcherException {
        PatternMatcher resourcePattern = PatternMatcher.createMysqlPattern(
                resourceName,
                CaseSensibility.RESOURCE.getCaseSensibility());

        Privilege.checkIncorrectPrivilege(Privilege.notBelongToResourcePrivileges, privs.toPrivilegeList());
        return new ResourcePrivEntry(resourcePattern,
                resourceName, privs);
    }

    public PatternMatcher getResourcePattern() {
        return resourcePattern;
    }

    public String getOrigResource() {
        return origResource;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof ResourcePrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        ResourcePrivEntry otherEntry = (ResourcePrivEntry) other;

        return origResource.compareTo(otherEntry.origResource);
    }

    @Override
    protected PrivEntry copy() throws AnalysisException, PatternMatcherException {
        return ResourcePrivEntry.create(this.getOrigResource(), this.getPrivSet().copy());
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof ResourcePrivEntry)) {
            return false;
        }

        ResourcePrivEntry otherEntry = (ResourcePrivEntry) other;
        if (origResource.equals(otherEntry.origResource)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("origResource:").append(origResource).append("priv:").append(privSet);
        return sb.toString();
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        origResource = Text.readString(in);
        try {
            resourcePattern = PatternMatcher.createMysqlPattern(origResource,
                    CaseSensibility.RESOURCE.getCaseSensibility());
        } catch (PatternMatcherException e) {
            throw new IOException(e);
        }
    }
}
