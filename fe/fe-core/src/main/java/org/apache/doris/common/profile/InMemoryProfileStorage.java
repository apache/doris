package org.apache.doris.common.profile;

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.ProfileManager.ProfileElement;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.common.util.RuntimeProfile;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.apache.doris.common.util.ProfileManager.PROFILE_HEADERS;
import static org.apache.doris.common.util.ProfileManager.QUERY_TYPE;

public class InMemoryProfileStorage implements ProfileStorage {
    private static final Logger LOG = LogManager.getLogger(InMemoryProfileStorage.class);

    private static final int ARRAY_SIZE = 100;
    // private static final int TOTAL_LEN = 1000 * ARRAY_SIZE ;

    private final ReadLock readLock;
    private final WriteLock writeLock;

    // record the order of profiles by queryId
    private final Deque<String> queryIdDeque;
    private final Map<String, ProfileElement> queryIdToProfileMap; // from QueryId to RuntimeProfile

    public InMemoryProfileStorage() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdDeque = new LinkedList<>();
        queryIdToProfileMap = new ConcurrentHashMap<>();
    }

    @Override
    public ProfileElement getProfile(String queryID) {
        readLock.lock();
        try {
            return queryIdToProfileMap.get(queryID);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<List<String>> getAllProfiles(ProfileType type) {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            Iterator<String> reverse = queryIdDeque.descendingIterator();
            while (reverse.hasNext()) {
                String  queryId = reverse.next();
                ProfileElement profileElement = queryIdToProfileMap.get(queryId);
                if (profileElement == null){
                    continue;
                }
                Map<String, String> infoStrings = profileElement.infoStrings;
                if (type != null && !infoStrings.get(QUERY_TYPE).equalsIgnoreCase(type.name())) {
                    continue;
                }

                List<String> row = Lists.newArrayList();
                for (String str : PROFILE_HEADERS) {
                    row.add(infoStrings.get(str));
                }
                result.add(row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    @Override
    public void pushProfile(RuntimeProfile profile) {
        if (profile == null) {
            return;
        }

        ProfileElement element = createElement(profile);
        String queryId = element.infoStrings.get(ProfileManager.QUERY_ID);
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(queryId)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' column into infoStrings");
        }

        // a profile may be updated multiple times in queryIdToProfileMap,
        // and only needs to be inserted into the queryIdDeque for the first time.
        queryIdToProfileMap.put(queryId, element);
        writeLock.lock();
        try {
            if (!queryIdDeque.contains(queryId)) {
                if (queryIdDeque.size() >= ARRAY_SIZE) {
                    queryIdToProfileMap.remove(queryIdDeque.getFirst());
                    queryIdDeque.removeFirst();
                }
                queryIdDeque.addLast(queryId);
            }
        } finally {
            writeLock.unlock();
        }
    }


    public ProfileElement createElement(RuntimeProfile profile) {
        ProfileElement element = new ProfileElement();
        RuntimeProfile summaryProfile = profile.getChildList().get(0).first;
        for (String header : PROFILE_HEADERS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        element.profileContent = profile.toString();

        MultiProfileTreeBuilder builder = new MultiProfileTreeBuilder(profile);
        try {
            builder.build();
        } catch (Exception e) {
            element.errMsg = e.getMessage();
            LOG.debug("failed to build profile tree", e);
            return element;
        }
        element.builder = builder;
        return element;
    }
}
