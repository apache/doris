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

#include "cloud/pb_convert.h"

#include <gen_cpp/olap_file.pb.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "google/protobuf/message.h"
#include "gtest/gtest.h"

namespace doris {

using namespace doris::cloud;
using namespace google::protobuf;

// RowsetMetaPB <=> RowsetMetaCloudPB
// TabletSchemaPB <=> TabletSchemaCloudPB
// TabletMetaPB <=> TabletMetaCloudPB

// test if 2 PBs have the same declared fields: count and names.
// note that reserved fields are not considered
bool have_same_fields(const google::protobuf::Descriptor* desc1,
                      const google::protobuf::Descriptor* desc2) {
    if (desc1->field_count() != desc2->field_count()) {
        return false;
    }
    std::set<std::string> fields1;
    for (int i = 0; i < desc1->field_count(); ++i) {
        fields1.insert(desc1->field(i)->name());
    }
    std::set<std::string> fields2;
    for (int i = 0; i < desc2->field_count(); ++i) {
        fields2.insert(desc2->field(i)->name());
    }
    return fields1 == fields2;
}

// traverse all fields of the given message clear them and set them to a default
// value, after which, all has_xxx() function will return true;
void set_all_fields_to_default(Message* message) {
    const Descriptor* descriptor = message->GetDescriptor();
    const Reflection* reflection = message->GetReflection();

    // set scalar value to the field
    auto set_scalar_type = [](Message* m, const FieldDescriptor* f, const Reflection* r) {
        switch (f->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            r->SetInt32(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            r->SetInt64(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            r->SetUInt32(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            r->SetUInt64(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            r->SetDouble(m, f, 0.0);
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            r->SetFloat(m, f, 0.0f);
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            r->SetBool(m, f, false);
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            r->SetEnum(m, f, f->enum_type()->value(0));
            break;
        case FieldDescriptor::CPPTYPE_STRING: {
            const std::string empty_str;
            r->SetString(m, f, empty_str);
            break;
        }
        default:
            EXPECT_TRUE(false) << "unexpected branch reached";
            break;
        }
    };

    // add a scalar value to the repeated field
    auto add_scalar_type = [](Message* m, const FieldDescriptor* f, const Reflection* r) {
        switch (f->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            r->AddInt32(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            r->AddInt64(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            r->AddUInt32(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            r->AddUInt64(m, f, 0);
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            r->AddDouble(m, f, 0.0);
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            r->AddFloat(m, f, 0.0f);
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            r->AddBool(m, f, false);
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            r->AddEnum(m, f, f->enum_type()->value(0));
            break;
        case FieldDescriptor::CPPTYPE_STRING: {
            const std::string empty_str;
            r->AddString(m, f, empty_str);
            break;
        }
        default:
            EXPECT_TRUE(false) << "unexpected branch reached";
            break;
        }
    };

    for (int i = 0; i < descriptor->field_count(); ++i) {
        const FieldDescriptor* field = descriptor->field(i);
        if (field->is_repeated()) { // add an element
            reflection->ClearField(message, field);
            if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                [[maybe_unused]] Message* sub_message = reflection->AddMessage(message, field);
                // the following has memory issue, however it is no need to set vaule
                // set_all_fields_to_default(sub_message);
            } else {
                add_scalar_type(message, field, reflection);
            }
        } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            Message* sub_message = reflection->MutableMessage(message, field);
            set_all_fields_to_default(sub_message);
        } else {
            set_scalar_type(message, field, reflection);
        }
    }
}

// get all names of fields that are set, despite of the vaules, of the msg
std::set<std::string> get_set_fields(const google::protobuf::Message& msg) {
    std::set<std::string> set_fields;
    const auto* descriptor = msg.GetDescriptor();
    const auto* reflection = msg.GetReflection();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->is_repeated()) {
            if (reflection->FieldSize(msg, field) > 0) {
                set_fields.insert(field->name());
            }
        } else {
            if (reflection->HasField(msg, field)) {
                set_fields.insert(field->name());
            }
        }
    }
    return set_fields;
}

// clang-format off

auto print = [](auto v) { std::stringstream s; for (auto& i : v) s << i << " "; return s.str(); };
auto set_diff = [](auto a, auto b) {
    std::set<std::string> r;
    std::set_difference(a.begin(), a.end(), b.begin(), b.end(), std::inserter(r, r.end()));
    return r;
};

// ensure that PBs need to be converted have identical fields, so that they can
// be inter-converted
TEST(PbConvert, ensure_identical_fields) {
    EXPECT_EQ(RowsetMetaPB::GetDescriptor()->field_count(), RowsetMetaCloudPB::GetDescriptor()->field_count());
    EXPECT_EQ(TabletSchemaPB::GetDescriptor()->field_count(), TabletSchemaCloudPB::GetDescriptor()->field_count());
    EXPECT_EQ(TabletMetaPB::GetDescriptor()->field_count(), TabletMetaCloudPB::GetDescriptor()->field_count());

    EXPECT_TRUE(have_same_fields(RowsetMetaPB::GetDescriptor(), RowsetMetaCloudPB::GetDescriptor()));
    EXPECT_TRUE(have_same_fields(TabletMetaPB::GetDescriptor(), TabletMetaCloudPB::GetDescriptor()));
    EXPECT_TRUE(have_same_fields(TabletSchemaPB::GetDescriptor(), TabletSchemaCloudPB::GetDescriptor()));
}

TEST(PbConvert, ensure_all_fields_converted_correctly) {
    // rowset meta
    RowsetMetaPB rs;
    set_all_fields_to_default(&rs);
    auto rowset_meta_set_fields = get_set_fields(rs);
    EXPECT_EQ(rowset_meta_set_fields.size(), RowsetMetaPB::GetDescriptor()->field_count()) << print(rowset_meta_set_fields);

    RowsetMetaCloudPB rs_cloud;
    set_all_fields_to_default(&rs_cloud);
    auto rowset_meta_cloud_set_fields = get_set_fields(rs_cloud);
    EXPECT_EQ(rowset_meta_cloud_set_fields.size(), RowsetMetaCloudPB::GetDescriptor()->field_count()) << print(rowset_meta_cloud_set_fields);
    EXPECT_EQ(rowset_meta_set_fields.size(), rowset_meta_cloud_set_fields.size());

    RowsetMetaCloudPB rowset_meta_cloud_out;
    doris_rowset_meta_to_cloud(&rowset_meta_cloud_out, rs);
    auto rowset_meta_cloud_out_set_fields = get_set_fields(rowset_meta_cloud_out);
    EXPECT_EQ(rowset_meta_set_fields.size(), rowset_meta_cloud_out_set_fields.size())
        << "doris_rowset_meta_to_cloud() missing output fields,"
        << "\n input_fields=" << print(rowset_meta_set_fields)
        << "\n output_fields=" << print(rowset_meta_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(rowset_meta_set_fields, rowset_meta_cloud_out_set_fields));

    RowsetMetaPB rowset_meta_out;
    cloud_rowset_meta_to_doris(&rowset_meta_out, rs_cloud);
    auto rowset_meta_out_set_fields = get_set_fields(rowset_meta_out);
    EXPECT_EQ(rowset_meta_cloud_set_fields.size(), rowset_meta_out_set_fields.size())
        << "cloud_rowset_meta_to_doris() missing output fields,"
        << "\n input_fields=" << print(rowset_meta_cloud_set_fields)
        << "\n output_fields=" << print(rowset_meta_out_set_fields)
        << "\n diff=" << print(set_diff(rowset_meta_cloud_set_fields, rowset_meta_out_set_fields));

    // tablet schema
    TabletSchemaPB tablet_schema;
    set_all_fields_to_default(&tablet_schema);
    auto tablet_schema_set_fields = get_set_fields(tablet_schema);
    EXPECT_EQ(tablet_schema_set_fields.size(), TabletSchemaPB::GetDescriptor()->field_count()) << print(tablet_schema_set_fields);

    TabletSchemaCloudPB tablet_schema_cloud;
    set_all_fields_to_default(&tablet_schema_cloud);
    auto tablet_schema_cloud_set_fields = get_set_fields(tablet_schema_cloud);
    EXPECT_EQ(tablet_schema_cloud_set_fields.size(), TabletSchemaCloudPB::GetDescriptor()->field_count()) << print(tablet_schema_cloud_set_fields);
    EXPECT_EQ(tablet_schema_set_fields.size(), tablet_schema_cloud_set_fields.size());

    TabletSchemaCloudPB tablet_schema_cloud_out;
    doris_tablet_schema_to_cloud(&tablet_schema_cloud_out, tablet_schema);
    auto tablet_schema_cloud_out_set_fields = get_set_fields(tablet_schema_cloud_out);
    EXPECT_EQ(tablet_schema_set_fields.size(), tablet_schema_cloud_out_set_fields.size())
        << "doris_tablet_schema_to_cloud() missing output fields,"
        << "\n input_fields=" << print(tablet_schema_set_fields)
        << "\n output_fields=" << print(tablet_schema_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_schema_set_fields, tablet_schema_cloud_out_set_fields));

    TabletSchemaPB tablet_schema_out;
    cloud_tablet_schema_to_doris(&tablet_schema_out, tablet_schema_cloud);
    auto tablet_schema_out_set_fields = get_set_fields(tablet_schema_out);
    EXPECT_EQ(tablet_schema_cloud_set_fields.size(), tablet_schema_out_set_fields.size())
        << "cloud_tablet_schema_to_doris() missing output fields,"
        << "\n input_fields=" << print(tablet_schema_cloud_set_fields)
        << "\n output_fields=" << print(tablet_schema_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_schema_cloud_set_fields, tablet_schema_out_set_fields));

    // tablet meta
    TabletMetaPB tablet_meta;
    set_all_fields_to_default(&tablet_meta);
    auto tablet_meta_set_fields = get_set_fields(tablet_meta);
    EXPECT_EQ(tablet_meta_set_fields.size(), TabletMetaPB::GetDescriptor()->field_count()) << print(tablet_meta_set_fields);
    TabletMetaCloudPB tablet_meta_cloud;
    set_all_fields_to_default(&tablet_meta_cloud);
    auto tablet_meta_cloud_set_fields = get_set_fields(tablet_meta_cloud);
    EXPECT_EQ(tablet_meta_cloud_set_fields.size(), TabletMetaCloudPB::GetDescriptor()->field_count()) << print(tablet_meta_cloud_set_fields);
    EXPECT_EQ(tablet_meta_set_fields.size(), tablet_meta_cloud_set_fields.size());

    TabletMetaCloudPB tablet_meta_cloud_out;
    doris_tablet_meta_to_cloud(&tablet_meta_cloud_out, tablet_meta);
    auto tablet_meta_cloud_out_set_fields = get_set_fields(tablet_meta_cloud_out);
    EXPECT_EQ(tablet_meta_set_fields.size(), tablet_meta_cloud_out_set_fields.size())
        << "doris_tablet_meta_to_cloud() missing output fields,"
        << "\n input_fields=" << print(tablet_meta_set_fields)
        << "\n output_fields=" << print(tablet_meta_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_meta_set_fields, tablet_meta_cloud_out_set_fields));

    TabletMetaPB tablet_meta_out;
    cloud_tablet_meta_to_doris(&tablet_meta_out, tablet_meta_cloud);
    auto tablet_meta_out_set_fields = get_set_fields(tablet_meta_out);
    EXPECT_EQ(tablet_meta_cloud_set_fields.size(), tablet_meta_out_set_fields.size())
        << "cloud_tablet_meta_to_doris() missing output fields,"
        << "\n input_fields=" << print(tablet_meta_cloud_set_fields)
        << "\n output_fields=" << print(tablet_meta_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_meta_cloud_set_fields, tablet_meta_out_set_fields));
}

TEST(PbConvert, test_rvalue_overloads) {
    // rowset meta
    RowsetMetaPB rs;
    set_all_fields_to_default(&rs);
    auto rs_set_fields = get_set_fields(rs);

    RowsetMetaCloudPB rs_cloud;
    set_all_fields_to_default(&rs_cloud);
    auto rs_cloud_set_fields = get_set_fields(rs_cloud);
    EXPECT_EQ(rs_set_fields.size(), rs_cloud_set_fields.size());

    RowsetMetaCloudPB rs_cloud_out;
    rs_cloud_out = doris_rowset_meta_to_cloud(std::move(rs));
    auto rs_cloud_out_set_fields = get_set_fields(rs_cloud_out);
    EXPECT_EQ(rs_set_fields.size(), rs_cloud_out_set_fields.size())
        << "doris_rowset_meta_to_cloud() missing output fields,"
        << "\n input_fields=" << print(rs_set_fields)
        << "\n output_fields=" << print(rs_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(rs_set_fields, rs_cloud_out_set_fields));

    RowsetMetaPB rs_out;
    rs_out = cloud_rowset_meta_to_doris(std::move(rs_cloud));
    auto rs_out_set_fields = get_set_fields(rs_out);
    EXPECT_EQ(rs_cloud_set_fields.size(), rs_out_set_fields.size())
        << "cloud_rowset_meta_to_doris() missing output fields,"
        << "\n input_fields=" << print(rs_cloud_set_fields)
        << "\n output_fields=" << print(rs_out_set_fields)
        << "\n diff=" << print(set_diff(rs_cloud_set_fields, rs_out_set_fields));

    // tablet schema
    TabletSchemaPB tablet_schema;
    set_all_fields_to_default(&tablet_schema);
    auto tablet_schema_set_fields = get_set_fields(tablet_schema);

    TabletSchemaCloudPB tablet_schema_cloud;
    set_all_fields_to_default(&tablet_schema_cloud);
    auto tablet_schema_cloud_set_fields = get_set_fields(tablet_schema_cloud);
    EXPECT_EQ(tablet_schema_set_fields.size(), tablet_schema_cloud_set_fields.size());

    TabletSchemaCloudPB tablet_schema_cloud_out;
    tablet_schema_cloud_out = doris_tablet_schema_to_cloud(std::move(tablet_schema));
    auto tablet_schema_cloud_out_set_fields = get_set_fields(tablet_schema_cloud_out);
    EXPECT_EQ(tablet_schema_set_fields.size(), tablet_schema_cloud_out_set_fields.size())
        << "doris_tablet_schema_to_cloud() missing output fields,"
        << "\n input_fields=" << print(tablet_schema_set_fields)
        << "\n output_fields=" << print(tablet_schema_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_schema_set_fields, tablet_schema_cloud_out_set_fields));

    TabletSchemaPB tablet_schema_out;
    tablet_schema_out = cloud_tablet_schema_to_doris(std::move(tablet_schema_cloud));
    auto tablet_schema_out_set_fields = get_set_fields(tablet_schema_out);
    EXPECT_EQ(tablet_schema_cloud_set_fields.size(), tablet_schema_out_set_fields.size())
        << "cloud_tablet_schema_to_doris() missing output fields,"
        << "\n input_fields=" << print(tablet_schema_cloud_set_fields)
        << "\n output_fields=" << print(tablet_schema_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_schema_cloud_set_fields, tablet_schema_out_set_fields));

    // tablet meta
    TabletMetaPB tablet_meta;
    set_all_fields_to_default(&tablet_meta);
    auto tablet_meta_set_fields = get_set_fields(tablet_meta);

    TabletMetaCloudPB tablet_meta_cloud;
    set_all_fields_to_default(&tablet_meta_cloud);
    auto tablet_meta_cloud_set_fields = get_set_fields(tablet_meta_cloud);
    EXPECT_EQ(tablet_meta_set_fields.size(), tablet_meta_cloud_set_fields.size());

    TabletMetaCloudPB tablet_meta_cloud_out;
    tablet_meta_cloud_out = doris_tablet_meta_to_cloud(std::move(tablet_meta));
    auto tablet_meta_cloud_out_set_fields = get_set_fields(tablet_meta_cloud_out);
    EXPECT_EQ(tablet_meta_set_fields.size(), tablet_meta_cloud_out_set_fields.size())
        << "doris_tablet_meta_to_cloud() missing output fields,"
        << "\n input_fields=" << print(tablet_meta_set_fields)
        << "\n output_fields=" << print(tablet_meta_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_meta_set_fields, tablet_meta_cloud_out_set_fields));

    TabletMetaPB tablet_meta_out;
    tablet_meta_out = cloud_tablet_meta_to_doris(std::move(tablet_meta_cloud));
    auto tablet_meta_out_set_fields = get_set_fields(tablet_meta_out);
    EXPECT_EQ(tablet_meta_cloud_set_fields.size(), tablet_meta_out_set_fields.size())
        << "cloud_tablet_meta_to_doris() missing output fields,"
        << "\n input_fields=" << print(tablet_meta_cloud_set_fields)
        << "\n output_fields=" << print(tablet_meta_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_meta_cloud_set_fields, tablet_meta_out_set_fields));
}

TEST(PbConvert, test_return_value_overloads) {
    // rowset meta
    RowsetMetaPB rs;
    set_all_fields_to_default(&rs);
    auto rs_set_fields = get_set_fields(rs);
    EXPECT_EQ(rs_set_fields.size(), RowsetMetaPB::GetDescriptor()->field_count());

    RowsetMetaCloudPB rs_cloud;
    set_all_fields_to_default(&rs_cloud);
    auto rs_cloud_set_fields = get_set_fields(rs_cloud);
    EXPECT_EQ(rs_cloud_set_fields.size(), RowsetMetaCloudPB::GetDescriptor()->field_count());
    EXPECT_EQ(rs_set_fields.size(), rs_cloud_set_fields.size());

    RowsetMetaCloudPB rs_cloud_out;
    rs_cloud_out = doris_rowset_meta_to_cloud(std::move(rs));
    auto rs_cloud_out_set_fields = get_set_fields(rs_cloud_out);
    EXPECT_EQ(rs_set_fields.size(), rs_cloud_out_set_fields.size())
        << "doris_rowset_meta_to_cloud() missing output fields,"
        << "\n input_fields=" << print(rs_set_fields)
        << "\n output_fields=" << print(rs_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(rs_set_fields, rs_cloud_out_set_fields));

    RowsetMetaPB rs_out;
    rs_out = cloud_rowset_meta_to_doris(std::move(rs_cloud));
    auto rs_out_set_fields = get_set_fields(rs_out);
    EXPECT_EQ(rs_cloud_set_fields.size(), rs_out_set_fields.size())
        << "cloud_rowset_meta_to_doris() missing output fields,"
        << "\n input_fields=" << print(rs_cloud_set_fields)
        << "\n output_fields=" << print(rs_out_set_fields)
        << "\n diff=" << print(set_diff(rs_cloud_set_fields, rs_out_set_fields));

    // tablet schema
    TabletSchemaPB tablet_schema;
    set_all_fields_to_default(&tablet_schema);
    auto tablet_schema_set_fields = get_set_fields(tablet_schema);
    EXPECT_EQ(tablet_schema_set_fields.size(), TabletSchemaPB::GetDescriptor()->field_count());

    TabletSchemaCloudPB tablet_schema_cloud;
    set_all_fields_to_default(&tablet_schema_cloud);
    auto tablet_schema_cloud_set_fields = get_set_fields(tablet_schema_cloud);
    EXPECT_EQ(tablet_schema_cloud_set_fields.size(), TabletSchemaCloudPB::GetDescriptor()->field_count());
    EXPECT_EQ(tablet_schema_set_fields.size(), tablet_schema_cloud_set_fields.size());

    TabletSchemaCloudPB tablet_schema_cloud_out;
    tablet_schema_cloud_out = doris_tablet_schema_to_cloud(std::move(tablet_schema));
    auto tablet_schema_cloud_out_set_fields = get_set_fields(tablet_schema_cloud_out);
    EXPECT_EQ(tablet_schema_set_fields.size(), tablet_schema_cloud_out_set_fields.size())
        << "doris_tablet_schema_to_cloud() missing output fields,"
        << "\n input_fields=" << print(tablet_schema_set_fields)
        << "\n output_fields=" << print(tablet_schema_cloud_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_schema_set_fields, tablet_schema_cloud_out_set_fields));

    TabletSchemaPB tablet_schema_out;
    tablet_schema_out = cloud_tablet_schema_to_doris(std::move(tablet_schema_cloud));
    auto tablet_schema_out_set_fields = get_set_fields(tablet_schema_out);
    EXPECT_EQ(tablet_schema_cloud_set_fields.size(), tablet_schema_out_set_fields.size())
        << "cloud_tablet_schema_to_doris() missing output fields,"
        << "\n input_fields=" << print(tablet_schema_cloud_set_fields)
        << "\n output_fields=" << print(tablet_schema_out_set_fields)
        << "\n diff=" << print(set_diff(tablet_schema_cloud_set_fields, tablet_schema_out_set_fields));
}

// clang-format on

} // namespace doris

// vim: et tw=80 ts=4 sw=4 cc=80:
