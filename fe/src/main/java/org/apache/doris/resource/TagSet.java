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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.Tag.Type;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * TagSet represents a set of tags.
 * TagSet is printed as { "type1" : "tag1,tag2", "type2" : "tag1" }
 * this class is not thread safe.
 */
public class TagSet {
    private Set<Tag> tags = Sets.newHashSet();

    private TagSet() {
    }

    private TagSet(TagSet other) {
        for (Tag tag : other.getAllTags()) {
            tags.add(tag);
        }
    }

    // create TagSet from tag map: { "type1" : "tag1,tag2", "type2" : "tag1" }
    public static TagSet create(Map<String, String> tagsMap) throws AnalysisException {
        TagSet tagSet = new TagSet();
        for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
            String typeStr = entry.getKey();
            String tagsStr = entry.getValue();
            String[] tagParts = tagsStr.split(",");
            for (String tagPart : tagParts) {
                Tag tag = Tag.create(typeStr, tagPart.trim());
                tagSet.addTag(tag);
            }
        }
        return tagSet;
    }

    public static TagSet create(String type, String tagName) throws AnalysisException {
        TagSet tagSet = new TagSet();
        Tag tag = Tag.create(type, tagName.trim());
        tagSet.addTag(tag);
        return tagSet;
    }

    public static TagSet create(Tag... tags) {
        TagSet tagSet = new TagSet();
        for (Tag tag : tags) {
            tagSet.addTag(tag);
        }
        return tagSet;
    }

    public boolean addTag(Tag tag) {
        return tags.add(tag);
    }

    public boolean deleteTag(Tag tag) {
        return tags.remove(tag);
    }

    // get a set of tags by tag type
    public TagSet getTagsByType(Tag.Type type) {
        TagSet tagSet = new TagSet();
        for (Tag tag : tags) {
            if (tag.type == type) {
                tagSet.addTag(tag);
            }
        }
        return tagSet;
    }

    public boolean containsTag(Tag tag) {
        return tags.contains(tag);
    }

    // merge 2 tag sets, the result is the union of 2 sets.
    public void merge(TagSet other) {
        for (Tag tag : other.tags) {
            addTag(tag);
        }
    }

    public Set<Tag.Type> getTypes() {
        Set<Tag.Type> set = Sets.newHashSet();
        for (Tag tag : tags) {
            set.add(tag.type);
        }
        return set;
    }

    private void deleteType(Type type) {
        tags = tags.stream().filter(t -> t.type != type).collect(Collectors.toSet());
    }

    // merge 2 tag sets, but all types of tag in this tagset will be substituted by type in 'other' tagset
    // eg:
    // tagset A: { "type1" : "tag1,tag2", "type2" : "tag1" }
    // tagset B: { "type1" : "tag3", "type3" : "tag4" }
    // result of A.substituteMerge(B): { "type1" : "tag3", "type2" : "tag1", "type3" : "tag4" }
    public void substituteMerge(TagSet other) {
        Set<Tag.Type> types = other.getTypes();
        for (Tag.Type type : types) {
            deleteType(type);
            merge(other.getTagsByType(type));
        }
    }

    public Set<Tag> getAllTags() {
        return tags;
    }

    public boolean isEmpty() {
        return tags.isEmpty();
    }

    @Override
    public String toString() {
        Map<String, String> map = Maps.newHashMap();
        Gson gson = new Gson();
        for (Tag.Type type : Tag.Type.values()) {
            TagSet tagSet = getTagsByType(type);
            if (!tagSet.isEmpty()) {
                map.put(type.toString(), Joiner.on(",").join(tagSet.getAllTags().stream().map(t -> t.tag).collect(Collectors.toList())));
            }
        }
        return gson.toJson(map);
    }
}
