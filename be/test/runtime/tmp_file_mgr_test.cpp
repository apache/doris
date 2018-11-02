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

#include "runtime/tmp_file_mgr.h"

#include <cstdlib>

#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "gen_cpp/Types_types.h"  // for TUniqueId

#include "util/disk_info.h"
#include "util/filesystem_util.h"
#include "util/logging.h"
#include "util/metrics.h"

using boost::filesystem::path;
using std::string;
using std::vector;
using std::set;

namespace doris {

class TmpFileMgrTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        _metrics.reset(new MetricRegistry(""));
    }

    virtual void TearDown() {
        _metrics.reset();
    }
#if 0
    // Check that metric values are consistent with TmpFileMgr state.
    void check_metrics(TmpFileMgr* tmp_file_mgr) {
        vector<TmpFileMgr::DeviceId> active = tmp_file_mgr->active_tmp_devices();
        IntCounter* active_metric = _metrics->get_metric<IntCounter>(
                "tmp-file-mgr.active-scratch-dirs");
        EXPECT_EQ(active.size(), active_metric->value());
        SetMetric<string>* active_set_metric = _metrics->get_metric<SetMetric<string> >(
                "tmp-file-mgr.active-scratch-dirs.list");
        set<string> active_set = active_set_metric->value();
        EXPECT_EQ(active.size(), active_set.size());
        for (int i = 0; i < active.size(); ++i) {
            string tmp_dir_path = tmp_file_mgr->get_tmp_dir_path(active[i]);
            EXPECT_TRUE(active_set.find(tmp_dir_path) != active_set.end());
        }
    }
#endif
    boost::scoped_ptr<MetricRegistry> _metrics;
};

// Regression test for IMPALA-2160. Verify that temporary file manager allocates blocks
// at the expected file offsets and expands the temporary file to the correct size.
TEST_F(TmpFileMgrTest, TestFileAllocation) {
    TmpFileMgr tmp_file_mgr;
    EXPECT_TRUE(tmp_file_mgr.init(_metrics.get()).ok());
    // Default configuration should give us one temporary device.
    EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
    vector<TmpFileMgr::DeviceId> tmp_devices = tmp_file_mgr.active_tmp_devices();
    EXPECT_EQ(1, tmp_devices.size());
    TUniqueId id;
    TmpFileMgr::File *file;
    Status status = tmp_file_mgr.get_file(tmp_devices[0], id, &file);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(file != NULL);
    // Apply writes of variable sizes and check space was allocated correctly.
    int64_t write_sizes[] = {
        1, 10, 1024, 4, 1024 * 1024 * 8, 1024 * 1024 * 8, 16, 10
    };
    int num_write_sizes = sizeof(write_sizes) / sizeof(write_sizes[0]);
    int64_t next_offset = 0;
    for (int i = 0; i < num_write_sizes; ++i) {
        int64_t offset;
        status = file->allocate_space(write_sizes[i], &offset);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(next_offset, offset);
        next_offset = offset + write_sizes[i];
        EXPECT_EQ(next_offset, boost::filesystem::file_size(file->path()));
    }
    // Check that cleanup is correct.
    status = file->remove();
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(boost::filesystem::exists(file->path()));
    // check_metrics(&tmp_file_mgr);
}
// Test that we can do initialization with two directories on same device and
// that validations prevents duplication of directories.
TEST_F(TmpFileMgrTest, TestOneDirPerDevice) {
    vector<string> tmp_dirs;
    tmp_dirs.push_back("/tmp/tmp-file-mgr-test.1");
    tmp_dirs.push_back("/tmp/tmp-file-mgr-test.2");
    for (int i = 0; i < tmp_dirs.size(); ++i) {
        EXPECT_TRUE(FileSystemUtil::create_directory(tmp_dirs[i]).ok());
    }
    TmpFileMgr tmp_file_mgr;
    tmp_file_mgr.init_custom(tmp_dirs, true, _metrics.get());

    // Only the first directory should be used.
    EXPECT_EQ(1, tmp_file_mgr.num_active_tmp_devices());
    vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
    EXPECT_EQ(1, devices.size());
    TUniqueId id;
    TmpFileMgr::File *file;
    EXPECT_TRUE(tmp_file_mgr.get_file(devices[0], id, &file).ok());
    // Check the prefix is the expected temporary directory.
    EXPECT_EQ(0, file->path().find(tmp_dirs[0]));
    FileSystemUtil::remove_paths(tmp_dirs);
    // check_metrics(&tmp_file_mgr);
}

// Test that we can do custom initialization with two dirs on same device.
TEST_F(TmpFileMgrTest, TestMultiDirsPerDevice) {
    vector<string> tmp_dirs;
    tmp_dirs.push_back("/tmp/tmp-file-mgr-test.1");
    tmp_dirs.push_back("/tmp/tmp-file-mgr-test.2");
    for (int i = 0; i < tmp_dirs.size(); ++i) {
        EXPECT_TRUE(FileSystemUtil::create_directory(tmp_dirs[i]).ok());
    }
    TmpFileMgr tmp_file_mgr;
    tmp_file_mgr.init_custom(tmp_dirs, false, _metrics.get());

    // Both directories should be used.
    EXPECT_EQ(2, tmp_file_mgr.num_active_tmp_devices());
    vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
    EXPECT_EQ(2, devices.size());
    for (int i = 0; i < tmp_dirs.size(); ++i) {
        EXPECT_EQ(0, tmp_file_mgr.get_tmp_dir_path(devices[i]).find(tmp_dirs[i]));
        TUniqueId id;
        TmpFileMgr::File *file;
        EXPECT_TRUE(tmp_file_mgr.get_file(devices[i], id, &file).ok());
        // Check the prefix is the expected temporary directory.
        EXPECT_EQ(0, file->path().find(tmp_dirs[i]));
    }
    FileSystemUtil::remove_paths(tmp_dirs);
    // check_metrics(&tmp_file_mgr);
}

// Test that reporting a write error is possible but does not result in
// blacklisting, which is disabled.
TEST_F(TmpFileMgrTest, TestReportError) {
    vector<string> tmp_dirs;
    tmp_dirs.push_back("/tmp/tmp-file-mgr-test.1");
    tmp_dirs.push_back("/tmp/tmp-file-mgr-test.2");
    for (int i = 0; i < tmp_dirs.size(); ++i) {
        EXPECT_TRUE(FileSystemUtil::create_directory(tmp_dirs[i]).ok());
    }
    TmpFileMgr tmp_file_mgr;
    tmp_file_mgr.init_custom(tmp_dirs, false, _metrics.get());

    // Both directories should be used.
    vector<TmpFileMgr::DeviceId> devices = tmp_file_mgr.active_tmp_devices();
    EXPECT_EQ(2, devices.size());
    // check_metrics(&tmp_file_mgr);

    // Inject an error on one device so that we can validate it is handled correctly.
    TUniqueId id;
    int good_device = 0;
    int bad_device = 1;
    TmpFileMgr::File* bad_file;
    EXPECT_TRUE(tmp_file_mgr.get_file(devices[bad_device], id, &bad_file).ok());
    // ErrorMsg errmsg(TErrorCode::GENERAL, "A fake error");
    // bad_file->ReportIOError(errmsg);
    bad_file->report_io_error("A fake error");

    // Blacklisting is disabled.
    EXPECT_FALSE(bad_file->is_blacklisted());
    // The second device should still be active.
    EXPECT_EQ(2, tmp_file_mgr.num_active_tmp_devices());
    vector<TmpFileMgr::DeviceId> devices_after = tmp_file_mgr.active_tmp_devices();
    EXPECT_EQ(2, devices_after.size());
    // check_metrics(&tmp_file_mgr);

    // Attempts to expand bad file should succeed.
    int64_t offset;
    EXPECT_TRUE(bad_file->allocate_space(128, &offset).ok());
    EXPECT_TRUE(bad_file->remove().ok());
    // The good device should still be usable.
    TmpFileMgr::File* good_file;
    EXPECT_TRUE(tmp_file_mgr.get_file(devices[good_device], id, &good_file).ok());
    EXPECT_TRUE(good_file != NULL);
    EXPECT_TRUE(good_file->allocate_space(128, &offset).ok());
    // Attempts to allocate new files on bad device should succeed.
    EXPECT_TRUE(tmp_file_mgr.get_file(devices[bad_device], id, &bad_file).ok());
    FileSystemUtil::remove_paths(tmp_dirs);
    // check_metrics(&tmp_file_mgr);
}

TEST_F(TmpFileMgrTest, TestAllocateFails) {
    string tmp_dir("/tmp/tmp-file-mgr-test.1");
    string scratch_subdir = tmp_dir + "/doris-scratch";
    vector<string> tmp_dirs(1, tmp_dir);
    EXPECT_TRUE(FileSystemUtil::create_directory(tmp_dir).ok());
    TmpFileMgr tmp_file_mgr;
    tmp_file_mgr.init_custom(tmp_dirs, false, _metrics.get());

    TUniqueId id;
    TmpFileMgr::File* allocated_file1;
    TmpFileMgr::File* allocated_file2;
    int64_t offset;
    EXPECT_TRUE(tmp_file_mgr.get_file(0, id, &allocated_file1).ok());
    EXPECT_TRUE(tmp_file_mgr.get_file(0, id, &allocated_file2).ok());
    EXPECT_TRUE(allocated_file1->allocate_space(1, &offset).ok());

    // Make scratch non-writable and test for allocation errors at different stages:
    // new file creation, files with no allocated blocks. files with allocated space.
    chmod(scratch_subdir.c_str(), 0);
    // allocated_file1 already has space allocated.
    EXPECT_FALSE(allocated_file1->allocate_space(1, &offset).ok());
    // allocated_file2 has no space allocated.
    EXPECT_FALSE(allocated_file2->allocate_space(1, &offset).ok());
    // Creating a new File object can succeed because it is not immediately created on disk.
    TmpFileMgr::File* unallocated_file;
    EXPECT_TRUE(tmp_file_mgr.get_file(0, id, &unallocated_file).ok());

    chmod(scratch_subdir.c_str(), S_IRWXU);
    FileSystemUtil::remove_paths(tmp_dirs);
}

} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::config::query_scratch_dirs = "/tmp";
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::DiskInfo::init();

    return RUN_ALL_TESTS();
}

