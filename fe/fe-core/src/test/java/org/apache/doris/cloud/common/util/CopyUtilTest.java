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

package org.apache.doris.cloud.common.util;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CopyUtilTest {
    public enum Status {
        ACTIVE, INACTIVE, PENDING
    }

    public static class Address {
        @SerializedName(value = "city")
        public String city;
        @SerializedName(value = "street")
        public String street;

        public Address(String city, String street) {
            this.city = city;
            this.street = street;
        }
    }

    public static class Parent implements Writable {
        @SerializedName(value = "name")
        public String name;
        @SerializedName(value = "age")
        public int age;
        @SerializedName(value = "salary")
        public double salary;
        @SerializedName(value = "isActive")
        public boolean isActive;
        @SerializedName(value = "status")
        public Status status;
        @SerializedName(value = "address")
        public Address address;
        @SerializedName(value = "hobbies")
        public List<String> hobbies;
        @SerializedName(value = "scores")
        public Set<Integer> scores;
        @SerializedName(value = "attributes")
        public Map<String, Object> attributes;

        public Parent() {
        }

        public Parent(String name, int age) {
            this.name = name;
            this.age = age;
            this.salary = 10000.50;
            this.isActive = true;
            this.status = Status.ACTIVE;
            this.address = new Address("Beijing", "XiErQi");
            this.hobbies = Arrays.asList("Reading", "Swimming", "Coding");
            this.scores = new HashSet<>(Arrays.asList(90, 95, 88));
            this.attributes = new HashMap<>();
            this.attributes.put("Department", "dev");
            this.attributes.put("Level", 3L);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }
    }

    public static class Child extends Parent {
        @SerializedName(value = "tag")
        public String tag;

        @SerializedName(value = "scr")
        public int scr;
        @SerializedName(value = "workAddresses")
        public List<Address> workAddresses;
        @SerializedName(value = "statusDescriptions")
        public Map<Status, String> statusDescriptions;

        public Child() {
            super();
        }

        public Child(String name, int age) {
            super(name, age);
            this.tag = "Child";
            this.scr = 100;
            this.workAddresses = Arrays.asList(
                new Address("Shanghai", "PuDong"),
                new Address("Shenzhen", "NanShan")
            );
            this.statusDescriptions = new EnumMap<>(Status.class);
            this.statusDescriptions.put(Status.ACTIVE, "active");
            this.statusDescriptions.put(Status.INACTIVE, "inactive");
        }
    }

    @Test
    public void testCopyToChildNormal() {
        Parent parent = new Parent("Alice", 30);
        Child child = CopyUtil.copyToChild(parent, Child.class);

        Assert.assertNotNull(child);
        Assert.assertEquals("Alice", child.name);
        Assert.assertEquals(30, child.age);
        Assert.assertEquals(10000.50, child.salary, 0.001);
        Assert.assertTrue(child.isActive);

        Assert.assertEquals(Status.ACTIVE, child.status);
        Assert.assertEquals("Beijing", child.address.city);
        Assert.assertEquals("XiErQi", child.address.street);

        Assert.assertEquals(3, child.hobbies.size());
        Assert.assertTrue(child.hobbies.contains("Reading"));
        Assert.assertEquals(3, child.scores.size());
        Assert.assertTrue(child.scores.contains(90));

        Assert.assertEquals(2, child.attributes.size());
        Assert.assertEquals("dev", child.attributes.get("Department"));
        Assert.assertEquals(3L, child.attributes.get("Level"));

        Assert.assertNull(child.tag);
        Assert.assertEquals(0, child.scr);
        Assert.assertNull(child.workAddresses);
        Assert.assertNull(child.statusDescriptions);

        Assert.assertEquals("Child", child.getClass().getSimpleName());
    }

    @Test
    public void testCopyToChildWithNullParent() {
        Child child = CopyUtil.copyToChild(null, Child.class);
        Assert.assertNull(child);
    }

    @Test
    public void testCopyToChildWithNullChildClass() {
        Parent parent = new Parent("Mike", 30);
        Child child = CopyUtil.copyToChild(parent, null);
        Assert.assertNull(child);
    }

    @Test
    public void testCopyToChildWithEmptyCollections() {
        Parent parent = new Parent();
        parent.hobbies = new ArrayList<>();
        parent.scores = new HashSet<>();
        parent.attributes = new HashMap<>();

        Child child = CopyUtil.copyToChild(parent, Child.class);
        Assert.assertNotNull(child);
        Assert.assertTrue(child.hobbies.isEmpty());
        Assert.assertTrue(child.scores.isEmpty());
        Assert.assertTrue(child.attributes.isEmpty());
    }
}

