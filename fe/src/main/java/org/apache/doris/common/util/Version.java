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

package org.apache.doris.common.util;

import java.util.Objects;

import org.apache.commons.lang.StringUtils;

/**
 * Parse software's version, like XX.YY.ZZ, where xx is major version, yy is minor version and ZZ is revision
 */
public class Version implements Comparable<Version> {

    public static final Version CURRENT_DORIS_VERSION = new Version(110000);
    // Plugin version is the same as Doris's history version
    public static final Version CURRENT_PLUGIN_VERSION = new Version(110000);

    public static final Version JDK_9_0_0 = new Version(9000000);
    public static final Version JDK_1_8_0 = new Version(1080000);

    public final int id;

    public final byte major;

    public final byte minor;

    public final byte revision;

    public Version(int id) {
        this.id = id;

        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
    }

    public Version(byte major, byte minor, byte revision) {
        this.major = major;
        this.minor = minor;
        this.revision = revision;

        this.id = major * 1000000 + minor * 10000 + revision * 100;
    }

    public static Version fromString(String version) throws IllegalArgumentException {

        if (StringUtils.isBlank(version)) {
            throw new IllegalArgumentException("Illegal empty version");
        }

        String[] list = version.split("[.]");

        if (list.length < 3) {
            throw new IllegalArgumentException("Illegal version format: " + version);
        }

        try {
            int major = Integer.parseInt(list[0].trim());
            int minor = Integer.parseInt(list[1].trim());
            int revision = Integer.parseInt(list[2].trim());

            if (major >= 100 || minor >= 100 || revision >= 100) {
                throw new IllegalArgumentException("Illegal version format: " + version);
            }

            return new Version((byte) major, (byte) minor, (byte) revision);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal version format: " + version, e);
        }
    }

    @Override
    public int compareTo(Version o) {
        return Integer.compare(this.id, o.id);
    }

    public boolean onOrAfter(Version v) {
        return this.id >= v.id;
    }

    public boolean after(Version v) {
        return this.id > v.id;
    }

    public boolean before(Version v) {
        return this.id < v.id;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + revision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Version version = (Version) o;
        return id == version.id &&
                major == version.major &&
                minor == version.minor &&
                revision == version.revision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, major, minor, revision);
    }
}
