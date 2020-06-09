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

package org.apache.doris.external.elasticsearch;


/**
 * Elasticsearch major version information, useful to check client's query compatibility with the Rest API.
 *
 * reference es-hadoop:
 *
 */
public class EsMajorVersion {
    public static final EsMajorVersion V_5_X = new EsMajorVersion((byte) 5, "5.x");
    public static final EsMajorVersion V_6_X = new EsMajorVersion((byte) 6, "6.x");
    public static final EsMajorVersion V_7_X = new EsMajorVersion((byte) 7, "7.x");
    public static final EsMajorVersion LATEST = V_7_X;

    public final byte major;
    private final String version;

    private EsMajorVersion(byte major, String version) {
        this.major = major;
        this.version = version;
    }

    public boolean after(EsMajorVersion version) {
        return version.major < major;
    }

    public boolean on(EsMajorVersion version) {
        return version.major == major;
    }

    public boolean notOn(EsMajorVersion version) {
        return !on(version);
    }

    public boolean onOrAfter(EsMajorVersion version) {
        return version.major <= major;
    }

    public boolean before(EsMajorVersion version) {
        return version.major > major;
    }

    public boolean onOrBefore(EsMajorVersion version) {
        return version.major >= major;
    }

    public static EsMajorVersion parse(String version) throws Exception {
        if (version.startsWith("5.")) {
            return new EsMajorVersion((byte) 5, version);
        }
        if (version.startsWith("6.")) {
            return new EsMajorVersion((byte) 6, version);
        }
        if (version.startsWith("7.")) {
            return new EsMajorVersion((byte) 7, version);
        }
        throw new Exception("Unsupported/Unknown Elasticsearch version [" + version + "]." +
                "Highest supported version is [" + LATEST.version + "]. You may need to upgrade ES-Hadoop.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EsMajorVersion version = (EsMajorVersion) o;

        return major == version.major &&
                version.equals(version.version);
    }

    @Override
    public int hashCode() {
        return major;
    }

    @Override
    public String toString() {
        return version;
    }
}
