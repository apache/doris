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
package org.apache.doris.demo.flink;


import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

public class User implements DorisDeserializationSchema {

    private String name;

    private Integer age;

    private String price;

    private String sale;

    public User(String name, Integer age, String price, String sale) {
        this.name = name;
        this.age = age;
        this.price = price;
        this.sale = sale;
    }

    public User() {
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getAge() {
        return age;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getSale() {
        return sale;
    }

    public void setSale(String sale) {
        this.sale = sale;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", price='" + price + '\'' +
                ", sale='" + sale + '\'' +
                '}';
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(List.class);
    }
}
