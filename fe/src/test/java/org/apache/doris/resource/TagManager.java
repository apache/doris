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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * TagManager maintains an inverted index from tag to resource.
 * The caller can get a set of resources based on a given set of Tags
 */
public class TagManager {
    // tag -> set of resource id
    private HashMultimap<Tag, Long> tagIndex = HashMultimap.create();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public boolean addIndex(Tag tag, Long resourceId) {
        lock.writeLock().lock();
        try {
            return tagIndex.put(tag, resourceId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean deleteIndex(Tag tag, Long resourceId) {
        lock.writeLock().lock();
        try {
            return tagIndex.remove(tag, resourceId);
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

    // get resource ids by the given set of tags
    public Set<Long> getResourceIdsByTags(TagSet tagSet) {
        if (tagSet.isEmpty()) {
            return Sets.newHashSet();
        }
        lock.readLock().lock();
        try {
            Set<Long> res = null;
            Set<Tag> tags = tagSet.getTags();
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
            return res;
        } finally {
            lock.readLock().unlock();
        }
    }
}
