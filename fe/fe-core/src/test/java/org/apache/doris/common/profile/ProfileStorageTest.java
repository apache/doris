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

package org.apache.doris.common.profile;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;

import org.junit.Assert;
import org.junit.Test;

public class ProfileStorageTest {
    @Test
    public void testProfileStorage() {
        ProfileStorage profileStorage = new InMemoryProfileStorage();
        insertDummyProfile(0, profileStorage);

        Assert.assertEquals(1, profileStorage.getAllProfiles(null).size());

        String content = profileStorage.getProfileContent("test_profile0");
        Assert.assertNotNull(content);
        try {
            Assert.assertNotNull(profileStorage.getProfileBuilder("test_profile0"));
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testProfileStorageOverflow() {
        ProfileStorage profileStorage = new InMemoryProfileStorage();
        for (int i = 0; i < 101; i++) {
            insertDummyProfile(i, profileStorage);
        }

        Assert.assertEquals(100, profileStorage.getAllProfiles(null).size());
        String content = profileStorage.getProfileContent("test_profile0");
        Assert.assertNull(content);
    }

    private void insertDummyProfile(int index, ProfileStorage storage) {
        // init profile
        RuntimeProfile profile = new RuntimeProfile("profile");
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        for (String header : ProfileManager.PROFILE_HEADERS) {
            profile1.addInfoString(header, "test_profile" + index);
        }
        profile.addChild(profile1);

        storage.pushProfile(profile);
    }
}
