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

package org.apache.doris.resource;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * TagManager maintains 2 indexes:
 *      one is from tag to resource.
 *      one is from resource to tags 
 * The caller can get a set of resources based on a given set of Tags
 */
public class TagManager implements Writable {
    // tag -> set of resource id
    private HashMultimap<Tag, Long> tagIndex = HashMultimap.create();

    @SerializedName(value = "resourceIndex")
    // resource id -> tag set
    private Map<Long, TagSet> resourceIndex = Maps.newHashMap();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public TagManager() {
        // TODO Auto-generated constructor stub
    }

    public boolean addResourceTag(Long resourceId, Tag tag) {
        lock.writeLock().lock();
        try {
            if (resourceIndex.containsKey(resourceId)) {
                resourceIndex.get(resourceId).addTag(tag);
            } else {
                resourceIndex.put(resourceId, TagSet.create(tag));
            }

            return tagIndex.put(tag, resourceId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addResourceTags(Long resourceId, TagSet tagSet) {
        if (tagSet.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            TagSet existTagSet = resourceIndex.get(resourceId);
            if (existTagSet == null) {
                existTagSet = TagSet.create();
                resourceIndex.put(resourceId, existTagSet);
            }
            existTagSet.union(tagSet);
            for (Tag tag : tagSet.getAllTags()) {
                tagIndex.put(tag, resourceId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // remove a resource and all its corresponding tags.
    // return true if resource exist.
    public boolean removeResource(Long resourceId) {
        lock.writeLock().lock();
        try {
            TagSet tagSet = resourceIndex.remove(resourceId);
            if (tagSet != null) {
                for (Tag tag : tagSet.getAllTags()) {
                    tagIndex.remove(tag, resourceId);
                }
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // remove a tag from specified resource.
    // return true if resource with this tag exist.
    public boolean removeResourceTag(Long resourceId, Tag tag) {
        lock.writeLock().lock();
        try {
            if (resourceIndex.containsKey(resourceId)) {
                TagSet tagSet = resourceIndex.get(resourceId);
                boolean res = tagSet.deleteTag(tag);
                if (tagSet.isEmpty()) {
                    resourceIndex.remove(resourceId);
                }

                tagIndex.remove(tag, resourceId);
                return res;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // remove set of tags from specified resource.
    public void removeResourceTags(Long resourceId, TagSet tagSet) {
        lock.writeLock().lock();
        try {
            if (resourceIndex.containsKey(resourceId)) {
                TagSet existingTagSet = resourceIndex.get(resourceId);
                for (Tag tag : tagSet.getAllTags()) {
                    existingTagSet.deleteTag(tag);
                    tagIndex.remove(tag, resourceId);
                }

                if (tagSet.isEmpty()) {
                    resourceIndex.remove(resourceId);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Set<Long> getResourceIdsByTag(Tag tag) {
        lock.readLock().lock();
        try {
            return Sets.newHashSet(tagIndex.get(tag));
        } finally {
            lock.readLock().unlock();
        }
    }

    // get resource ids by the given set of tags.
    // The relationship between these tags is "AND".
    // return a empty set if tag is empty or no resource meets requirement.
    public Set<Long> getResourceIdsByTags(TagSet tagSet) {
        if (tagSet.isEmpty()) {
            return Sets.newHashSet();
        }
        lock.readLock().lock();
        try {
            Set<Long> res = null;
            Set<Tag> tags = tagSet.getAllTags();
            for (Tag tag : tags) {
                if (res == null) {
                    res = Sets.newHashSet(tagIndex.get(tag));
                } else {
                    res.retainAll(tagIndex.get(tag));
                }
                if (res.isEmpty()) {
                    // if the result is already empty, break immediately
                    break;
                }
            }
            return res == null ? Sets.newHashSet() : res;
        } finally {
            lock.readLock().unlock();
        }
    }

    // when replayed from edit log, tagIndex need to be built based on resourceIndex
    private void rebuildTagIndex() {
        for (Map.Entry<Long, TagSet> entry : resourceIndex.entrySet()) {
            long resourceId = entry.getKey();
            for (Tag tag : entry.getValue().getAllTags()) {
                tagIndex.put(tag, resourceId);
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static TagManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        TagManager tagManager = GsonUtils.GSON.fromJson(json, TagManager.class);
        tagManager.rebuildTagIndex();
        return tagManager;
    }
}
