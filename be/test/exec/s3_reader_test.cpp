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

#include "exec/s3_reader.h"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <map>
#include <string>
#include <vector>

#include "exec/s3_writer.h"

namespace doris {
static const std::string AK = "";
static const std::string SK = "";
static const std::string ENDPOINT = "http://s3.bj.bcebos.com";
static const std::string REGION = "bj";
static const std::string BUCKET = "s3://yang-repo/";
class S3ReaderTest : public testing::Test {
public:
    S3ReaderTest()
            : _aws_properties({{"AWS_ACCESS_KEY", AK},
                               {"AWS_SECRET_KEY", SK},
                               {"AWS_ENDPOINT", ENDPOINT},
                               {"AWS_REGION", REGION}}) {
        _s3_base_path = BUCKET + "s3/" + gen_uuid();
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
    std::string gen_uuid() {
        auto id = boost::uuids::random_generator()();
        return boost::lexical_cast<std::string>(id);
    }
    std::map<std::string, std::string> _aws_properties;
    std::string _s3_base_path;
    std::string _content =
            "O wild West Wind, thou breath of Autumn's being\n"
            "Thou, from whose unseen presence the leaves dead\n"
            "Are driven, like ghosts from an enchanter fleeing,\n"
            "Yellow, and black, and pale, and hectic red,\n"
            "Pestilence-stricken multitudes:O thou\n"
            "Who chariotest to their dark wintry bed\n"
            "The winged seeds, where they lie cold and low,\n"
            "Each like a corpse within its grave, until\n"
            "Thine azure sister of the Spring shall blow\n"
            "Her clarion o'er the dreaming earth, and fill\n"
            "(Driving sweet buds like flocks to feed in air)\n"
            "With living hues and odors plain and hill:\n"
            "Wild Spirit, which art moving everywhere;\n"
            "Destroyer and preserver; hear, oh, hear!";
};

TEST_F(S3ReaderTest, normal) {
    std::string path = _s3_base_path + "/test_file";
    std::unique_ptr<S3Writer> writer(new S3Writer(_aws_properties, path, 0));
    auto st = writer->open();
    ASSERT_TRUE(st.ok());
    size_t l = 0;
    st = writer->write(reinterpret_cast<const uint8_t*>(_content.c_str()), _content.length(), &l);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(_content.length(), l);
    st = writer->close();
    ASSERT_TRUE(st.ok());
    std::unique_ptr<S3Writer> writer1(new S3Writer(_aws_properties, path, 0));
    st = writer1->open();
    ASSERT_TRUE(st.is_already_exist());
    std::unique_ptr<S3Reader> reader(new S3Reader(_aws_properties, path, 0));
    st = reader->open();
    ASSERT_TRUE(st.ok());
    std::unique_ptr<S3Reader> reader1(new S3Reader(_aws_properties, path + "xx", 0));
    st = reader1->open();
    ASSERT_TRUE(st.is_not_found());
    ASSERT_EQ(_content.length(), reader->size());
    std::string verification_contents;
    verification_contents.resize(_content.length());
    int64_t total_read = 0;
    bool eof = false;
    st = reader->read((uint8_t*)&verification_contents[0], _content.length(), &total_read, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(_content, verification_contents);
    ASSERT_EQ(_content.length(), total_read);
    ASSERT_FALSE(eof);
    st = reader->read((uint8_t*)&verification_contents[0], _content.length(), &total_read, &eof);
    ASSERT_TRUE(eof);
    int64_t t = 0;
    st = reader->tell(&t);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(_content.length(), t);
    st = reader->readat(_content.length(), _content.length(), (int64_t*)(&total_read),
                        (uint8_t*)&verification_contents[0]);
    LOG(INFO) << total_read;
    ASSERT_TRUE(total_read == 0);
}
} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    int ret = 0;
    // ak sk is secret
    // ret = RUN_ALL_TESTS();
    return ret;
}
