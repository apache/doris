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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vec/columns/column_const.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {

struct DataTypeTestCases {
    FieldVector field_values;
    std::vector<String> expect_values;
};

TEST(ToStringMethodTest, DataTypeToStringTest) {
    // prepare field
    DataTypeTestCases cases;
    DataTypes data_types;
    std::vector<TypeIndex> type_ids = {TypeIndex::Int16, TypeIndex::String, TypeIndex::Decimal32};
    Array a1, a2;
    a1.push_back(UInt64(123));
    a1.push_back(Null());
    a1.push_back(UInt64(12345678));
    a1.push_back(UInt64(0));
    a2.push_back(Field(String("hello amory")));
    a2.push_back(Field("NULL"));
    a2.push_back(Field(String("cute amory")));
    a2.push_back(Null());
    Map m;
    m.push_back(a1);
    m.push_back(a2);

    Tuple t;
    t.push_back(Int128(12345454342));
    t.push_back(Field(String("amory cute")));
    t.push_back(UInt64(0));

    cases.field_values = {UInt64(12),
                          Field(String(" hello amory , cute amory ")),
                          DecimalField<Decimal32>(-12345678, 0),
                          a1,
                          a2,
                          t,
                          m};
    cases.expect_values = {"12",
                           " hello amory , cute amory ",
                           "-12345678",
                           "[123, NULL, 12345678, 0]",
                           "['hello amory', 'NULL', 'cute amory', 'NULL']",
                           "{12345454342, amory cute, 0}",
                           "{123:\"hello amory\", null:\"NULL\", 12345678:\"cute amory\", 0:null}"};

    for (const auto id : type_ids) {
        const auto data_type = DataTypeFactory::instance().create_data_type(id);
        data_types.push_back(data_type);
    }

    // complex type
    DataTypePtr n1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr n3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
    DataTypePtr s1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr u1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());

    DataTypePtr a = std::make_shared<DataTypeArray>(u1);
    data_types.push_back(a);
    DataTypePtr au = std::make_shared<DataTypeArray>(s1);
    data_types.push_back(au);
    DataTypes dataTypes;
    dataTypes.push_back(n3);
    dataTypes.push_back(s1);
    dataTypes.push_back(u1);

    // data_type_struct
    DataTypePtr s = std::make_shared<DataTypeStruct>(dataTypes);
    data_types.push_back(s);

    DataTypePtr mt = std::make_shared<DataTypeMap>(u1, s1);
    data_types.push_back(mt);

    for (int i = 0; i < data_types.size(); ++i) {
        DataTypePtr data_type = data_types[i];
        std::cout << i << " : " << data_type->get_name() << std::endl;
        const auto field = cases.field_values[i];
        ColumnPtr col = data_type->create_column_const(1, field);
        std::cout << "col name:" << col->get_name() << std::endl;
        std::string to = data_type->to_string(*col, 1);
        std::string expect = cases.expect_values[i];
        //        std::cout << "expect: " << expect << " to : " << to << std::endl;
        ASSERT_EQ(cases.expect_values[i], to);
    }
}
} // namespace doris::vectorized