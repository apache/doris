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

#include "olap/metadata_adder.h"

#include <gtest/gtest.h>

#include "olap/tablet_schema.h"

namespace doris {

class MetadataAdderTest : public testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}

    MetadataAdderTest() = default;
    ~MetadataAdderTest() override = default;
};

template <typename T>
void test_construct_new_obj() {
    int classSize = sizeof(T);

    std::cout << "Class Name: " << typeid(T).name() << std::endl;

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);
    // 1 default construct
    {
        T t1;
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize);
        T t2;
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);
    // 2 copy construct
    {
        T t1;
        T t2 = t1;
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);

    // 3 move construct
    {
        T t1;
        T t2 = std::move(t1);
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);

    // 4 copy assignment
    {
        T t1;
        T t2;
        int before_size = MetadataAdder<T>::get_all_tablets_size();
        t1 = t2;
        ASSERT_TRUE(before_size == MetadataAdder<T>::get_all_tablets_size());
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);

    // 5 move assignment
    {
        T t1;
        T t2;
        int before_size = MetadataAdder<T>::get_all_tablets_size();
        t1 = std::move(t2);
        ASSERT_TRUE(before_size == MetadataAdder<T>::get_all_tablets_size());
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);
}

TEST_F(MetadataAdderTest, metadata_adder_test) {
    test_construct_new_obj<TabletSchema>();
    test_construct_new_obj<TabletColumn>();
    test_construct_new_obj<TabletIndex>();
}

}; // namespace doris