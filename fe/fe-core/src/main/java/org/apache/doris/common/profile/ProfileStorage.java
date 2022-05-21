package org.apache.doris.common.profile;

import org.apache.doris.common.util.ProfileManager.ProfileElement;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.common.util.RuntimeProfile;

import java.util.List;

public interface ProfileStorage {
    ProfileElement getProfile(String queryID);
    public List<List<String>> getAllProfiles(ProfileType type);

     void pushProfile(RuntimeProfile profile) ;

}
