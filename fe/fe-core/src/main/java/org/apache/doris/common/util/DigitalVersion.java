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

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Parse software's version, like XX.YY.ZZ, where xx is major version, yy is minor version and ZZ is revision
 */
public class DigitalVersion implements Comparable<DigitalVersion> {

    public static final DigitalVersion CURRENT_DORIS_VERSION = new DigitalVersion(110000);
    // Plugin version is the same as Doris's history version
    public static final DigitalVersion CURRENT_PLUGIN_VERSION = new DigitalVersion(110000);

    public static final DigitalVersion JDK_9_0_0 = new DigitalVersion(9000000);
    public static final DigitalVersion JDK_1_8_0 = new DigitalVersion(1080000);

    @SerializedName("id")
    public final int id;

    @SerializedName("major")
    public final byte major;

    @SerializedName("minor")
    public final byte minor;

    @SerializedName("revision")
    public final byte revision;

    public DigitalVersion(int id) {
        this.id = id;

        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
    }

    public DigitalVersion(byte major, byte minor, byte revision) {
        this.major = major;
        this.minor = minor;
        this.revision = revision;

        this.id = major * 1000000 + minor * 10000 + revision * 100;
    }

    public static DigitalVersion fromString(String version) throws IllegalArgumentException {

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
                throw new IllegalArgumentException(
                        "Illegal version format: " + version + ". Expected: major.minor.revision");
            }

            return new DigitalVersion((byte) major, (byte) minor, (byte) revision);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal version format: " + version, e);
        }
    }

    @Override
    public int compareTo(DigitalVersion o) {
        return Integer.compare(this.id, o.id);
    }

    public boolean onOrAfter(DigitalVersion v) {
        return this.id >= v.id;
    }

    public boolean after(DigitalVersion v) {
        return this.id > v.id;
    }

    public boolean before(DigitalVersion v) {
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
        DigitalVersion version = (DigitalVersion) o;
        return id == version.id
                && major == version.major
                && minor == version.minor
                && revision == version.revision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, major, minor, revision);
    }
}
