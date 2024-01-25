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

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>
#include <stdint.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_builder.h"
#include "runtime/exec_env.h"
#include "util/jni-util.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

#ifndef CHECK_STATUS_OK
#define CHECK_STATUS_OK(stmt)                   \
    do {                                        \
        Status _status_ = (stmt);               \
        ASSERT_TRUE(_status_.ok()) << _status_; \
    } while (false)
#endif

// set your own info
// s3
static std::string ak = "";
static std::string sk = "";
static std::string endpoint = "http://cos.ap-beijing.myqcloud.com";
static std::string region = "ap-beijing";
static std::string s3_location = "";

// hdfs
static std::string fs_name = "hdfs://my_nameservice";
static std::string username = "hadoop";
static std::string nameservices = "my_nameservice";
static std::string nn = "nn1,nn2";
static std::string rpc1 = "172.21.0.1:4007";
static std::string rpc2 = "172.21.0.2:4007";
static std::string provider =
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
static std::string hdfs_location = "/user/doris/";

// broker
static std::string broker_ip = "127.0.0.1";
static int broker_port = 8008;
static std::string broker_location = "hdfs://my_nameservice/user/doris";

class RemoteFileSystemTest : public testing::Test {
public:
    virtual void SetUp() {
        s3_prop.emplace("AWS_ACCESS_KEY", ak);
        s3_prop.emplace("AWS_SECRET_KEY", sk);
        s3_prop.emplace("AWS_ENDPOINT", endpoint);
        s3_prop.emplace("AWS_REGION", region);

        hdfs_prop.emplace("fs.defaultFS", fs_name);
        hdfs_prop.emplace("hadoop.username", username);
        hdfs_prop.emplace("username", username); // for broker hdfs
        hdfs_prop.emplace("dfs.nameservices", nameservices);
        hdfs_prop.emplace("dfs.ha.namenodes." + nameservices, nn);
        hdfs_prop.emplace("dfs.namenode.rpc-address." + nameservices + ".nn1", rpc1);
        hdfs_prop.emplace("dfs.namenode.rpc-address." + nameservices + ".nn2", rpc2);
        hdfs_prop.emplace("dfs.client.failover.proxy.provider." + nameservices, provider);

        broker_addr.__set_hostname(broker_ip);
        broker_addr.__set_port(broker_port);
        CHECK_STATUS_OK(doris::JniUtil::Init());
    }

    virtual void TearDown() {}

private:
    std::map<std::string, std::string> s3_prop;
    std::map<std::string, std::string> hdfs_prop;
    TNetworkAddress broker_addr;
};

TEST_F(RemoteFileSystemTest, TestBrokerFileSystem) {
    std::shared_ptr<io::BrokerFileSystem> fs;
    CHECK_STATUS_OK(io::BrokerFileSystem::create(broker_addr, hdfs_prop, &fs));

    // delete directory
    io::Path delete_path = broker_location + "/tmp1";
    CHECK_STATUS_OK(fs->delete_directory(delete_path));
    io::Path delete_path2 = broker_location + "/tmp2";
    CHECK_STATUS_OK(fs->delete_directory(delete_path2));
    // create directory not implemented
    // io::Path create_path = delete_path;
    // CHECK_STATUS_OK(fs->create_directory(create_path));
    // write file
    std::string file1 = broker_location + "/tmp1/file1.txt";
    io::FileWriterPtr writer;
    CHECK_STATUS_OK(fs->create_file(file1, &writer));
    CHECK_STATUS_OK(writer->append({"content"}));
    CHECK_STATUS_OK(writer->close());
    // read file
    io::FileReaderSPtr reader;
    CHECK_STATUS_OK(fs->open_file(file1, &reader));
    char read_buf[10];
    size_t bytes_read = 0;
    CHECK_STATUS_OK(reader->read_at(0, {read_buf, 10}, &bytes_read));
    ASSERT_EQ(7, bytes_read);

    // exist
    bool exists = false;
    CHECK_STATUS_OK(fs->exists(file1, &exists));
    ASSERT_TRUE(exists);
    std::string file_non_exist = broker_location + "/non-exist";
    CHECK_STATUS_OK(fs->exists(file_non_exist, &exists));
    ASSERT_FALSE(exists);
    CHECK_STATUS_OK(fs->exists(delete_path, &exists));
    ASSERT_TRUE(exists);

    // file size
    int64_t file_size = 0;
    CHECK_STATUS_OK(fs->file_size(file1, &file_size));
    // file size is not implemented
    ASSERT_EQ(0, file_size);

    // write more files
    for (int i = 0; i < 10; i++) {
        std::string tmp_file = fmt::format(broker_location + "/tmp1/tmp_file_{}", i);
        io::FileWriterPtr writer;
        CHECK_STATUS_OK(fs->create_file(tmp_file, &writer));
        CHECK_STATUS_OK(writer->append({"content"}));
        CHECK_STATUS_OK(writer->close());
    }

    // list files
    std::vector<io::FileInfo> files;
    CHECK_STATUS_OK(fs->list(delete_path, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(11, files.size());
    for (auto& file_info : files) {
        std::cout << "file name: " << file_info.file_name << std::endl;
        ASSERT_EQ(7, file_info.file_size);
        ASSERT_TRUE(file_info.is_file);
    }
    std::string non_exist_path = broker_location + "/non_exist/";
    files.clear();
    CHECK_STATUS_OK(fs->list(non_exist_path, true, &files, &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, files.size());

    // rename
    std::string src_name = file1;
    std::string dst_name = broker_location + "/tmp1/new_file1.txt";
    CHECK_STATUS_OK(fs->rename(src_name, dst_name));
    CHECK_STATUS_OK(fs->exists(src_name, &exists));
    ASSERT_FALSE(exists);
    CHECK_STATUS_OK(fs->exists(dst_name, &exists));
    ASSERT_TRUE(exists);

    // rename dir
    std::string src_dir = delete_path;
    std::string dst_dir = broker_location + "/tmp2";
    CHECK_STATUS_OK(fs->rename_dir(src_dir, dst_dir));
    CHECK_STATUS_OK(fs->exists(dst_name, &exists));
    ASSERT_FALSE(exists);
    std::string new_dst_name = dst_dir + "/new_file1.txt";
    CHECK_STATUS_OK(fs->exists(new_dst_name, &exists));
    ASSERT_TRUE(exists);

    // batch delete
    std::vector<io::Path> delete_files;
    for (int i = 0; i < 10; i++) {
        std::string tmp_file = fmt::format(broker_location + "/tmp2/tmp_file_{}", i);
        delete_files.emplace_back(tmp_file);
        CHECK_STATUS_OK(fs->batch_delete(delete_files));
    }

    // list to check
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(1, files.size());

    // upload
    std::string upload_file = "./be/test/io/fs/test_data/upload_file.txt";
    std::string remote_file = dst_dir + "/upload_file.txt";
    CHECK_STATUS_OK(fs->upload(upload_file, remote_file));
    exists = false;
    CHECK_STATUS_OK(fs->exists(remote_file, &exists));
    ASSERT_TRUE(exists);

    // batch upload
    std::vector<io::Path> local_files;
    std::vector<io::Path> remote_files;
    for (int i = 0; i < 10; i++) {
        local_files.push_back(upload_file);
        remote_files.push_back(fmt::format(dst_dir + "/upload_file_{}", i));
    }
    CHECK_STATUS_OK(fs->batch_upload(local_files, remote_files));
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(12, files.size());
    for (auto& file : remote_files) {
        CHECK_STATUS_OK(fs->file_size(file, &file_size));
        // file size is not implemented
        ASSERT_EQ(0, file_size);
    }

    // direct_upload
    std::string direct_remote_file = dst_dir + "/direct_upload_file";
    CHECK_STATUS_OK(fs->direct_upload(direct_remote_file, "abc"));
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(13, files.size());
    CHECK_STATUS_OK(fs->file_size(direct_remote_file, &file_size));
    ASSERT_EQ(0, file_size);

    // download
    std::string local_download_file = "./be/test/io/fs/test_data/download_file.txt";
    CHECK_STATUS_OK(io::global_local_filesystem()->delete_file(local_download_file));
    CHECK_STATUS_OK(fs->download(direct_remote_file, local_download_file));
    CHECK_STATUS_OK(io::global_local_filesystem()->file_size(local_download_file, &file_size));
    ASSERT_EQ(3, file_size);

    // direct download
    std::string download_content;
    CHECK_STATUS_OK(fs->direct_download(direct_remote_file, &download_content));
    ASSERT_EQ("abc", download_content);
}

TEST_F(RemoteFileSystemTest, TestHdfsFileSystem) {
    THdfsParams hdfs_params = parse_properties(hdfs_prop);
    std::shared_ptr<io::HdfsFileSystem> fs;
    CHECK_STATUS_OK(io::HdfsFileSystem::create(hdfs_params, hdfs_location, &fs));

    // delete directory
    io::Path delete_path = "tmp1";
    CHECK_STATUS_OK(fs->delete_directory(delete_path));
    io::Path delete_path2 = "tmp2";
    CHECK_STATUS_OK(fs->delete_directory(delete_path2));
    // create directory
    io::Path create_path = delete_path;
    CHECK_STATUS_OK(fs->create_directory(create_path));
    // write file
    std::string file1 = "tmp1/file1.txt";
    io::FileWriterPtr writer;
    CHECK_STATUS_OK(fs->create_file(file1, &writer));
    CHECK_STATUS_OK(writer->append({"content"}));
    CHECK_STATUS_OK(writer->close());
    // read file
    io::FileReaderSPtr reader;
    CHECK_STATUS_OK(fs->open_file(file1, &reader));
    char read_buf[10];
    size_t bytes_read = 0;
    CHECK_STATUS_OK(reader->read_at(0, {read_buf, 10}, &bytes_read));
    ASSERT_EQ(7, bytes_read);

    // exist
    bool exists = false;
    CHECK_STATUS_OK(fs->exists(file1, &exists));
    ASSERT_TRUE(exists);
    std::string file_non_exist = "non-exist";
    CHECK_STATUS_OK(fs->exists(file_non_exist, &exists));
    ASSERT_FALSE(exists);
    CHECK_STATUS_OK(fs->exists(delete_path, &exists));
    ASSERT_TRUE(exists);

    // file size
    int64_t file_size = 0;
    CHECK_STATUS_OK(fs->file_size(file1, &file_size));
    ASSERT_EQ(7, file_size);

    // write more files
    for (int i = 0; i < 10; i++) {
        std::string tmp_file = fmt::format("tmp1/tmp_file_{}", i);
        io::FileWriterPtr writer;
        CHECK_STATUS_OK(fs->create_file(tmp_file, &writer));
        CHECK_STATUS_OK(writer->append({"content"}));
        CHECK_STATUS_OK(writer->close());
    }

    // list files
    std::vector<io::FileInfo> files;
    CHECK_STATUS_OK(fs->list(delete_path, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(11, files.size());
    for (auto& file_info : files) {
        std::cout << "file name: " << file_info.file_name << std::endl;
        ASSERT_EQ(7, file_info.file_size);
        ASSERT_TRUE(file_info.is_file);
    }
    std::string non_exist_path = "non_exist/";
    files.clear();
    CHECK_STATUS_OK(fs->list(non_exist_path, true, &files, &exists));
    ASSERT_FALSE(exists);
    ASSERT_EQ(0, files.size());

    // rename
    std::string src_name = file1;
    std::string dst_name = "tmp1/new_file1.txt";
    CHECK_STATUS_OK(fs->rename(src_name, dst_name));
    CHECK_STATUS_OK(fs->exists(src_name, &exists));
    ASSERT_FALSE(exists);
    CHECK_STATUS_OK(fs->exists(dst_name, &exists));
    ASSERT_TRUE(exists);

    // rename dir
    std::string src_dir = delete_path;
    std::string dst_dir = "tmp2";
    CHECK_STATUS_OK(fs->rename_dir(src_dir, dst_dir));
    CHECK_STATUS_OK(fs->exists(dst_name, &exists));
    ASSERT_FALSE(exists);
    std::string new_dst_name = dst_dir + "/new_file1.txt";
    CHECK_STATUS_OK(fs->exists(new_dst_name, &exists));
    ASSERT_TRUE(exists);

    // batch delete
    std::vector<io::Path> delete_files;
    for (int i = 0; i < 10; i++) {
        std::string tmp_file = fmt::format("tmp2/tmp_file_{}", i);
        delete_files.emplace_back(tmp_file);
        CHECK_STATUS_OK(fs->batch_delete(delete_files));
    }

    // list to check
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(1, files.size());

    // upload
    std::string upload_file = "./be/test/io/fs/test_data/upload_file.txt";
    std::string remote_file = dst_dir + "/upload_file.txt";
    CHECK_STATUS_OK(fs->upload(upload_file, remote_file));
    exists = false;
    CHECK_STATUS_OK(fs->exists(remote_file, &exists));
    ASSERT_TRUE(exists);

    // batch upload
    std::vector<io::Path> local_files;
    std::vector<io::Path> remote_files;
    for (int i = 0; i < 10; i++) {
        local_files.push_back(upload_file);
        remote_files.push_back(fmt::format(dst_dir + "/upload_file_{}", i));
    }
    CHECK_STATUS_OK(fs->batch_upload(local_files, remote_files));
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(12, files.size());
    for (auto& file : remote_files) {
        CHECK_STATUS_OK(fs->file_size(file, &file_size));
        ASSERT_EQ(11, file_size);
    }

    // direct_upload
    std::string direct_remote_file = dst_dir + "/direct_upload_file";
    CHECK_STATUS_OK(fs->direct_upload(direct_remote_file, "abc"));
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(13, files.size());
    CHECK_STATUS_OK(fs->file_size(direct_remote_file, &file_size));
    ASSERT_EQ(3, file_size);

    // download
    std::string local_download_file = "./be/test/io/fs/test_data/download_file.txt";
    CHECK_STATUS_OK(io::global_local_filesystem()->delete_file(local_download_file));
    CHECK_STATUS_OK(fs->download(direct_remote_file, local_download_file));
    CHECK_STATUS_OK(io::global_local_filesystem()->file_size(local_download_file, &file_size));
    ASSERT_EQ(3, file_size);

    // direct download
    std::string download_content;
    CHECK_STATUS_OK(fs->direct_download(direct_remote_file, &download_content));
    ASSERT_EQ("abc", download_content);
}

TEST_F(RemoteFileSystemTest, TestS3FileSystem) {
    std::unique_ptr<ThreadPool> _pool;
    ThreadPoolBuilder("S3FileUploadThreadPool")
            .set_min_threads(5)
            .set_max_threads(10)
            .build(&_pool);
    ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(_pool);
    S3Conf s3_conf;
    S3URI s3_uri(s3_location);
    CHECK_STATUS_OK(s3_uri.parse());
    CHECK_STATUS_OK(S3ClientFactory::convert_properties_to_s3_conf(s3_prop, s3_uri, &s3_conf));
    std::shared_ptr<io::S3FileSystem> fs;
    CHECK_STATUS_OK(io::S3FileSystem::create(std::move(s3_conf), "", &fs));

    // delete directory
    io::Path delete_path = s3_location + "/tmp1";
    CHECK_STATUS_OK(fs->delete_directory(delete_path));
    io::Path delete_path2 = s3_location + "/tmp2";
    CHECK_STATUS_OK(fs->delete_directory(delete_path2));
    // create directory
    io::Path create_path = delete_path;
    CHECK_STATUS_OK(fs->create_directory(create_path));
    // write file
    std::string file1 = s3_location + "/tmp1/file1.txt";
    io::FileWriterPtr writer;
    CHECK_STATUS_OK(fs->create_file(file1, &writer));
    CHECK_STATUS_OK(writer->append({"content"}));
    CHECK_STATUS_OK(writer->close());
    // read file
    io::FileReaderSPtr reader;
    CHECK_STATUS_OK(fs->open_file(file1, &reader));
    char read_buf[10];
    size_t bytes_read = 0;
    CHECK_STATUS_OK(reader->read_at(0, {read_buf, 10}, &bytes_read));
    ASSERT_EQ(7, bytes_read);

    // exist
    bool exists = false;
    CHECK_STATUS_OK(fs->exists(file1, &exists));
    ASSERT_TRUE(exists);
    std::string file_non_exist = s3_location + "/non-exist";
    CHECK_STATUS_OK(fs->exists(file_non_exist, &exists));
    ASSERT_FALSE(exists);
    CHECK_STATUS_OK(fs->exists(delete_path, &exists));
    ASSERT_FALSE(exists);

    // file size
    int64_t file_size = 0;
    CHECK_STATUS_OK(fs->file_size(file1, &file_size));
    ASSERT_EQ(7, file_size);

    // write more files
    for (int i = 0; i < 10; i++) {
        std::string tmp_file = fmt::format(s3_location + "/tmp1/tmp_file_{}", i);
        io::FileWriterPtr writer;
        CHECK_STATUS_OK(fs->create_file(tmp_file, &writer));
        CHECK_STATUS_OK(writer->append({"content"}));
        CHECK_STATUS_OK(writer->close());
    }

    // list files
    std::vector<io::FileInfo> files;
    CHECK_STATUS_OK(fs->list(delete_path, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(11, files.size());
    for (auto& file_info : files) {
        std::cout << "file name: " << file_info.file_name << std::endl;
        ASSERT_EQ(7, file_info.file_size);
        ASSERT_TRUE(file_info.is_file);
    }
    std::string non_exist_path = s3_location + "/non_exist/";
    files.clear();
    CHECK_STATUS_OK(fs->list(non_exist_path, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(0, files.size());

    // rename
    std::string src_name = file1;
    std::string dst_name = s3_location + "/tmp1/new_file1.txt";
    CHECK_STATUS_OK(fs->rename(src_name, dst_name));
    CHECK_STATUS_OK(fs->exists(src_name, &exists));
    ASSERT_FALSE(exists);
    CHECK_STATUS_OK(fs->exists(dst_name, &exists));
    ASSERT_TRUE(exists);

    // rename dir
    std::string src_dir = delete_path;
    std::string dst_dir = s3_location + "/tmp2";
    CHECK_STATUS_OK(fs->rename_dir(src_dir, dst_dir));
    CHECK_STATUS_OK(fs->exists(dst_name, &exists));
    ASSERT_FALSE(exists);
    std::string new_dst_name = dst_dir + "/new_file1.txt";
    CHECK_STATUS_OK(fs->exists(new_dst_name, &exists));
    ASSERT_TRUE(exists);

    // batch delete
    std::vector<io::Path> delete_files;
    for (int i = 0; i < 10; i++) {
        std::string tmp_file = fmt::format(s3_location + "/tmp2/tmp_file_{}", i);
        delete_files.emplace_back(tmp_file);
        CHECK_STATUS_OK(fs->batch_delete(delete_files));
    }

    // list to check
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(1, files.size());

    // upload
    std::string upload_file = "./be/test/io/fs/test_data/upload_file.txt";
    std::string remote_file = dst_dir + "/upload_file.txt";
    CHECK_STATUS_OK(fs->upload(upload_file, remote_file));
    exists = false;
    CHECK_STATUS_OK(fs->exists(remote_file, &exists));
    ASSERT_TRUE(exists);

    // batch upload
    std::vector<io::Path> local_files;
    std::vector<io::Path> remote_files;
    for (int i = 0; i < 10; i++) {
        local_files.push_back(upload_file);
        remote_files.push_back(fmt::format(dst_dir + "/upload_file_{}", i));
    }
    CHECK_STATUS_OK(fs->batch_upload(local_files, remote_files));
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(12, files.size());
    for (auto& file : remote_files) {
        CHECK_STATUS_OK(fs->file_size(file, &file_size));
        ASSERT_EQ(11, file_size);
    }

    // direct_upload
    std::string direct_remote_file = dst_dir + "/direct_upload_file";
    CHECK_STATUS_OK(fs->direct_upload(direct_remote_file, "abc"));
    files.clear();
    CHECK_STATUS_OK(fs->list(dst_dir, true, &files, &exists));
    ASSERT_TRUE(exists);
    ASSERT_EQ(13, files.size());
    CHECK_STATUS_OK(fs->file_size(direct_remote_file, &file_size));
    ASSERT_EQ(3, file_size);

    // download
    std::string local_download_file = "./be/test/io/fs/test_data/download_file.txt";
    CHECK_STATUS_OK(io::global_local_filesystem()->delete_file(local_download_file));
    CHECK_STATUS_OK(fs->download(direct_remote_file, local_download_file));
    CHECK_STATUS_OK(io::global_local_filesystem()->file_size(local_download_file, &file_size));
    ASSERT_EQ(3, file_size);

    // direct download
    std::string download_content;
    CHECK_STATUS_OK(fs->direct_download(direct_remote_file, &download_content));
    ASSERT_EQ("abc", download_content);
}

} // namespace doris
