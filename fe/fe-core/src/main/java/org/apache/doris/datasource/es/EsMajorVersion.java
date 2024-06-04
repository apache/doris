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

package org.apache.doris.datasource.es;


/**
 * Elasticsearch major version information, useful to check client's query compatibility with the Rest API.
 * <p>
 * reference es-hadoop:
 */
public class EsMajorVersion {

    public static final EsMajorVersion V_0_X = new EsMajorVersion((byte) 0, "0.x");
    public static final EsMajorVersion V_1_X = new EsMajorVersion((byte) 1, "1.x");
    public static final EsMajorVersion V_2_X = new EsMajorVersion((byte) 2, "2.x");
    public static final EsMajorVersion V_5_X = new EsMajorVersion((byte) 5, "5.x");
    public static final EsMajorVersion V_6_X = new EsMajorVersion((byte) 6, "6.x");
    public static final EsMajorVersion V_7_X = new EsMajorVersion((byte) 7, "7.x");
    public static final EsMajorVersion V_8_X = new EsMajorVersion((byte) 8, "8.x");
    public static final EsMajorVersion LATEST = V_8_X;

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

    public static EsMajorVersion parse(String version) throws DorisEsException {
        if (version.startsWith("0.")) {
            return new EsMajorVersion((byte) 0, version);
        }
        if (version.startsWith("1.")) {
            return new EsMajorVersion((byte) 1, version);
        }
        if (version.startsWith("2.")) {
            return new EsMajorVersion((byte) 2, version);
        }
        if (version.startsWith("5.")) {
            return new EsMajorVersion((byte) 5, version);
        }
        if (version.startsWith("6.")) {
            return new EsMajorVersion((byte) 6, version);
        }
        if (version.startsWith("7.")) {
            return new EsMajorVersion((byte) 7, version);
        }
        // used for the next released ES version
        if (version.startsWith("8.")) {
            return new EsMajorVersion((byte) 8, version);
        }
        throw new DorisEsException(
                "Unsupported/Unknown ES Cluster version [" + version + "]." + "Highest supported version is ["
                        + LATEST.version + "].");
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

        return major == version.major && version.equals(version.version);
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
