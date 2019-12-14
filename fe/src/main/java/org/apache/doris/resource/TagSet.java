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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * TagSet represents a set of tags.
 * TagSet is printed as { "type1" : "value1,value2", "type2" : "value1" }
 * TagSet is mutable and not thread safe
 */
public class TagSet implements Writable {
    public static final TagSet EMPTY_TAGSET = new TagSet();

    @SerializedName(value = "tags")
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

    // create from a single tag
    public static TagSet create(String type, String tagName) throws AnalysisException {
        TagSet tagSet = new TagSet();
        Tag tag = Tag.create(type, tagName.trim());
        tagSet.addTag(tag);
        return tagSet;
    }

    // create from multi tags
    public static TagSet create(Tag... tags) {
        TagSet tagSet = new TagSet();
        for (Tag tag : tags) {
            tagSet.addTag(tag);
        }
        return tagSet;
    }

    public static TagSet copyFrom(TagSet other) {
        TagSet tagSet = new TagSet();
        for (Tag tag : other.tags) {
            tagSet.addTag(tag);
        }
        return tagSet;
    }

    // return true if tag doesn't exist
    public boolean addTag(Tag tag) {
        return this.tags.add(tag);
    }

    // return true if tag exist
    public boolean deleteTag(Tag tag) {
        return tags.remove(tag);
    }

    // get a set of tags by tag type
    public TagSet getTagsByType(String type) {
        type = type.toLowerCase();
        TagSet tagSet = new TagSet();
        for (Tag tag : tags) {
            if (tag.type.equals(type)) {
                tagSet.addTag(tag);
            }
        }
        return tagSet;
    }

    public boolean containsTag(Tag tag) {
        return tags.contains(tag);
    }

    // the result is the union of 2 sets.
    public void union(TagSet other) {
        for (Tag tag : other.tags) {
            addTag(tag);
        }
    }

    // return all types in this tag set
    public Set<String> getTypes() {
        Set<String> set = Sets.newHashSet();
        for (Tag tag : tags) {
            set.add(tag.type);
        }
        return set;
    }

    // delete all tags of specified type
    private void deleteType(String type) {
        final String lowerType = type.toLowerCase();
        tags = tags.stream().filter(t -> !t.type.equals(lowerType)).collect(Collectors.toSet());
    }

    // merge 2 tag sets, but all types of tag in target tag set will be substituted by type in 'other' tagset
    // eg:
    // tagset A: { "type1" : "val1,val2", "type2" : "val1" }
    // tagset B: { "type1" : "val3", "type3" : "val4" }
    // result of A.substituteMerge(B): { "type1" : "val3", "type2" : "val1", "type3" : "val4" }
    public void substituteMerge(TagSet other) {
        Set<String> types = other.getTypes();
        for (String type : types) {
            deleteType(type);
            union(other.getTagsByType(type));
        }
    }

    public Set<Tag> getAllTags() {
        return tags;
    }

    public boolean isEmpty() {
        return tags.isEmpty();
    }

    // print as { "type1" : "tag1,tag2", "type2" : "tag1" }
    @Override
    public String toString() {
        Map<String, String> map = Maps.newHashMap();
        Gson gson = new Gson();
        for (String type : getTypes()) {
            TagSet tagSet = getTagsByType(type);
            if (!tagSet.isEmpty()) {
                map.put(type, Joiner.on(",").join(
                        tagSet.getAllTags().stream().map(t -> t.value).collect(Collectors.toList())));
            }
        }
        return gson.toJson(map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tags);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TagSet)) {
            return false;
        }
        TagSet tagSet = (TagSet) obj;
        return tags.equals(tagSet.tags);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TagSet read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TagSet.class);
    }
}
