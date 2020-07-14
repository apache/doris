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
import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ResourcePrivEntry extends PrivEntry {
    protected static final String ANY_RESOURCE = "*";

    protected PatternMatcher resourcePattern;
    protected String origResource;
    protected boolean isAnyResource;

    protected ResourcePrivEntry() {
    }

    protected ResourcePrivEntry(PatternMatcher hostPattern, String origHost, PatternMatcher resourcePattern, String origResource,
                                PatternMatcher userPattern, String user, boolean isDomain, PrivBitSet privSet) {
        super(hostPattern, origHost, userPattern, user, isDomain, privSet);
        this.resourcePattern = resourcePattern;
        this.origResource = origResource;
        if (origResource.equals(ANY_RESOURCE)) {
            isAnyResource = true;
        }
    }

    public static ResourcePrivEntry create(String host, String resourceName, String user, boolean isDomain, PrivBitSet privs)
            throws AnalysisException {
        PatternMatcher hostPattern = PatternMatcher.createMysqlPattern(host, CaseSensibility.HOST.getCaseSensibility());
        PatternMatcher resourcePattern = PatternMatcher.createMysqlPattern(resourceName.equals(ANY_RESOURCE) ? "%" : resourceName,
                                                                      CaseSensibility.RESOURCE.getCaseSensibility());
        PatternMatcher userPattern = PatternMatcher.createMysqlPattern(user, CaseSensibility.USER.getCaseSensibility());
        if (privs.containsNodePriv() || privs.containsDbTablePriv()) {
            throw new AnalysisException("Resource privilege can not contains node or db table privileges: " + privs);
        }
        return new ResourcePrivEntry(hostPattern, host, resourcePattern, resourceName, userPattern, user, isDomain, privs);
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
        int res = origHost.compareTo(otherEntry.origHost);
        if (res != 0) {
            return -res;
        }

        res = origResource.compareTo(otherEntry.origResource);
        if (res != 0) {
            return -res;
        }

        return -origUser.compareTo(otherEntry.origUser);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof ResourcePrivEntry)) {
            return false;
        }

        ResourcePrivEntry otherEntry = (ResourcePrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && origUser.equals(otherEntry.origUser)
                && origResource.equals(otherEntry.origResource) && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("resource priv. host: ").append(origHost).append(", resource: ").append(origResource);
        sb.append(", user: ").append(origUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = ResourcePrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);
        Text.writeString(out, origResource);
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        origResource = Text.readString(in);
        try {
            resourcePattern = PatternMatcher.createMysqlPattern(origResource, CaseSensibility.RESOURCE.getCaseSensibility());
        } catch (AnalysisException e) {
            throw new IOException(e);
        }
        isAnyResource = origResource.equals(ANY_RESOURCE);
    }
}
