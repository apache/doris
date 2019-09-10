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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/*
 * TagSet represents a set of tags.
 * TagSet is printed as "TYPE1:tag1,tag2#TYPE2:tag3,tag4..."
 * TagSet is immutable, each time a new tag is added or deleted, a new TagSet will be created.
 * So, this class is thread safe.
 */
public class TagSet {

    private Set<Tag> tags = Sets.newHashSet();

    private TagSet() {

    }

    private TagSet(TagSet other) {
        for (Tag tag : other.getTags()) {
            tags.add(tag);
        }
    }

    // create TagSet from string format: "TYPE1:tag1,tag2#TYPE2:tag3,tag4..."
    public static TagSet create(String tagsFormat) throws AnalysisException {
        TagSet tagSet = new TagSet();
        String[] split = tagsFormat.split("#");
        for (String part : split) {
            String[] tagPart = part.split(":");
            if (tagPart.length != 2) {
                throw new AnalysisException("Invalid tag format: " + part);
            }
            String[] tagNamePart = tagPart[1].split(",");
            for (String tagName : tagNamePart) {
                Tag tag = Tag.create(tagPart[0], tagName);
                tagSet.addTag(tag);
            }
        }
        return tagSet;
    }

    public static TagSet create(Tag... tags) {
        TagSet tagSet = new TagSet();
        for (Tag tag : tags) {
            tagSet.addTag(tag);
        }
        return tagSet;
    }

    public TagSet addTag(Tag tag) {
        if (tags.contains(tag)) {
            return this;
        }
        TagSet tagSet = new TagSet(this);
        tagSet.tags.add(tag);
        return tagSet;
    }

    public TagSet deleteTag(Tag tag) {
        if (!tags.contains(tag)) {
            return this;
        }
        TagSet tagSet = new TagSet(this);
        tagSet.tags.remove(tag);
        return tagSet;
    }

    // get a set of tags by tag type
    public TagSet getTags(Tag.Type type) {
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

    public Set<Tag> getTags() {
        return tags;
    }

    public boolean isEmpty() {
        return tags.isEmpty();
    }

    @Override
    public String toString() {
        List<String> list = Lists.newArrayList();
        for (Tag.Type type : Tag.Type.values()) {
            TagSet tagSet = getTags(type);
            if (!tagSet.isEmpty()) {
                list.add(type.toString() + ":" + Joiner.on(",").join(tagSet.getTags()));
            }
        }
        return Joiner.on("#").join(list);
    }
}
