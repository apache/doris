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

#include "exec/sink/writer/paimon/doris_paimon_file_system.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>

#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "util/defer_op.h"

namespace doris {
namespace {
struct Store {
    std::map<std::string, std::string> objects;
    Status write_error;
    Status close_error;
    Status list_error;
    std::string delete_failure_path;
    int closed = 0;
    int destroyed = 0;
    bool delayed_commit = false;
};

class TestWriter final : public io::FileWriter {
public:
    TestWriter(io::Path path, std::shared_ptr<Store> store)
            : _path(std::move(path)), _store(std::move(store)) {}
    ~TestWriter() override { ++_store->destroyed; }
    Status appendv(const Slice* slices, size_t count) override {
        if (!_store->write_error.ok()) return _store->write_error;
        for (size_t i = 0; i < count; ++i) _bytes.append(slices[i].data, slices[i].size);
        return Status::OK();
    }
    Status close(bool non_block) override {
        EXPECT_FALSE(non_block);
        ++_store->closed;
        _state = State::CLOSED;
        if (!_store->close_error.ok()) return _store->close_error;
        _store->objects[_path.native()] = _bytes;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes.size(); }
    State state() const override { return _state; }

private:
    io::Path _path;
    std::shared_ptr<Store> _store;
    std::string _bytes;
    State _state = State::OPENED;
};

class TestReader final : public io::FileReader {
public:
    TestReader(io::Path path, std::string bytes)
            : _path(std::move(path)), _bytes(std::move(bytes)) {}
    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _bytes.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice buffer, size_t* count, const io::IOContext*) override {
        *count = offset >= _bytes.size() ? 0 : std::min(buffer.size, _bytes.size() - offset);
        if (*count > 0) memcpy(buffer.data, _bytes.data() + offset, *count);
        return Status::OK();
    }

private:
    io::Path _path;
    std::string _bytes;
    bool _closed = false;
};

// Mimics Doris object storage: HEAD is file-only, LIST returns recursive relative keys
// and reports exists=true even when the prefix has no objects. No cloud service needed.
class TestObjectStore final : public io::FileSystem {
public:
    TestObjectStore()
            : FileSystem("paimon-test", io::FileSystemType::S3), store(std::make_shared<Store>()) {}
    std::shared_ptr<Store> store;

protected:
    Status create_file_impl(const io::Path& path, io::FileWriterPtr* out,
                            const io::FileWriterOptions* opts) override {
        store->delayed_commit = opts && opts->used_by_s3_committer;
        *out = std::make_unique<TestWriter>(path, store);
        return Status::OK();
    }
    Status open_file_impl(const io::Path& path, io::FileReaderSPtr* out,
                          const io::FileReaderOptions*) override {
        auto it = store->objects.find(path.native());
        if (it == store->objects.end()) return Status::NotFound("missing object");
        *out = std::make_shared<TestReader>(path, it->second);
        return Status::OK();
    }
    Status create_directory_impl(const io::Path&, bool) override { return Status::OK(); }
    Status delete_file_impl(const io::Path& path) override {
        if (path.native() == store->delete_failure_path)
            return Status::IOError("injected delete failure");
        store->objects.erase(path.native());
        return Status::OK();
    }
    Status delete_directory_impl(const io::Path& path) override {
        std::string prefix = path.native() + "/";
        std::erase_if(store->objects,
                      [&](const auto& item) { return item.first.starts_with(prefix); });
        return Status::OK();
    }
    Status batch_delete_impl(const std::vector<io::Path>& paths) override {
        for (const auto& path : paths) store->objects.erase(path.native());
        return Status::OK();
    }
    Status exists_impl(const io::Path& path, bool* exists) const override {
        *exists = store->objects.contains(path.native());
        return Status::OK();
    }
    Status file_size_impl(const io::Path& path, int64_t* size) const override {
        auto it = store->objects.find(path.native());
        if (it == store->objects.end()) return Status::NotFound("missing object");
        *size = it->second.size();
        return Status::OK();
    }
    Status list_impl(const io::Path& path, bool, std::vector<io::FileInfo>* files,
                     bool* exists) override {
        if (!store->list_error.ok()) return store->list_error;
        *exists = true;
        std::string prefix = path.native() + "/";
        for (const auto& [key, bytes] : store->objects) {
            if (key.starts_with(prefix))
                files->push_back(
                        {key.substr(prefix.size()), static_cast<int64_t>(bytes.size()), true});
        }
        return Status::OK();
    }
    Status rename_impl(const io::Path&, const io::Path&) override {
        return Status::NotSupported("rename");
    }
    Status absolute_path(const io::Path& path, io::Path& absolute) const override {
        absolute = path;
        return Status::OK();
    }
};
} // namespace

TEST(DorisPaimonFileSystemTest, ObjectWriteReadAndLogicalNamespace) {
    auto fs = std::make_shared<TestObjectStore>();
    DorisPaimonFileSystem adapter(fs, "oss://bucket/table", "s3://bucket/table", nullptr);
    auto created = adapter.Create("oss://bucket/table/data/a", false);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    auto out = std::move(created).value();
    ASSERT_TRUE(out->Write("abcdef", 6).ok());
    EXPECT_EQ(6, out->GetPos().value());
    EXPECT_EQ("oss://bucket/table/data/a", out->GetUri().value());
    EXPECT_TRUE(fs->store->objects.empty());
    ASSERT_TRUE(out->Flush().ok());
    ASSERT_TRUE(out->Close().ok());
    ASSERT_TRUE(out->Close().ok());
    EXPECT_EQ(1, fs->store->closed);
    EXPECT_EQ(1, fs->store->destroyed);
    EXPECT_FALSE(fs->store->delayed_commit);
    EXPECT_EQ("abcdef", fs->store->objects.at("s3://bucket/table/data/a"));
    EXPECT_TRUE(adapter.Create("oss://bucket/table/data/a", false).status().IsExist());
    auto opened = adapter.Open("oss://bucket/table/data/a");
    ASSERT_TRUE(opened.ok());
    auto in = std::move(opened).value();
    char bytes[8] {};
    ASSERT_TRUE(in->Seek(2, paimon::FS_SEEK_SET).ok());
    EXPECT_EQ(2, in->Read(bytes, 2).value());
    EXPECT_EQ("cd", std::string(bytes, 2));
    EXPECT_EQ(2, in->Read(bytes, 8, 4).value());
    EXPECT_EQ(4, in->GetPos().value()); // pread must not move the cursor
    EXPECT_FALSE(in->Seek(-7, paimon::FS_SEEK_END).ok());
    int callbacks = 0;
    in->ReadAsync(bytes, 2, 0, [&](paimon::Status status) {
        EXPECT_TRUE(status.ok());
        ++callbacks;
    });
    EXPECT_EQ(1, callbacks);
    ASSERT_TRUE(in->Close().ok());
    EXPECT_FALSE(in->Read(bytes, 1).ok());
}

TEST(DorisPaimonFileSystemTest, LocalWriteReadAndLogicalNamespace) {
    char directory[] = "/tmp/doris_paimon_fs_XXXXXX";
    ASSERT_NE(nullptr, mkdtemp(directory));
    auto fs = io::global_local_filesystem();
    Defer cleanup {[&] { EXPECT_TRUE(fs->delete_directory(directory).ok()); }};
    const std::string storage_root = std::string(directory) + "/table with spaces+%20?#";
    const std::string table_root = "file:" + storage_root;
    DorisPaimonFileSystem adapter(fs, table_root, storage_root, nullptr);
    const auto logical_path = table_root + "/data/part";
    ASSERT_TRUE(adapter.WriteFile(logical_path, "data", false).ok());

    bool exists = false;
    ASSERT_TRUE(fs->exists(storage_root + "/data/part", &exists).ok());
    ASSERT_TRUE(exists);
    auto opened = adapter.Open(logical_path);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    auto in = std::move(opened).value();
    EXPECT_EQ(logical_path, in->GetUri().value());
    char bytes[4] {};
    ASSERT_EQ(4, in->Read(bytes, sizeof(bytes)).value());
    EXPECT_EQ("data", std::string(bytes, sizeof(bytes)));
    ASSERT_TRUE(in->Close().ok());
    ASSERT_TRUE(adapter.cleanup_owned_files().ok());
    ASSERT_TRUE(fs->exists(storage_root + "/data/part", &exists).ok());
    EXPECT_FALSE(exists);
}

TEST(DorisPaimonFileSystemTest, PrefixListingAndDeleteScope) {
    auto fs = std::make_shared<TestObjectStore>();
    fs->store->objects = {{"s3://bucket/table/schema/schema-0", "schema"},
                          {"s3://bucket/table/bucket-0/a", "a"},
                          {"s3://bucket/table/bucket-0/b", "b"},
                          {"s3://bucket/table2/keep", "keep"}};
    DorisPaimonFileSystem adapter(fs, "oss://bucket/table", "s3://bucket/table", nullptr);
    std::vector<std::unique_ptr<paimon::BasicFileStatus>> list;
    ASSERT_TRUE(adapter.ListDir("oss://bucket/table", &list).ok());
    ASSERT_EQ(2, list.size());
    EXPECT_EQ("oss://bucket/table/bucket-0", list[0]->GetPath());
    EXPECT_TRUE(list[0]->IsDir());
    EXPECT_TRUE(adapter.Exists("oss://bucket/table/schema").value());
    EXPECT_FALSE(adapter.Exists("oss://bucket/table/missing").value());
    EXPECT_EQ(6, adapter.GetFileStatus("oss://bucket/table/schema/schema-0").value()->GetLen());
    EXPECT_FALSE(adapter.Delete("oss://bucket/table/schema", false).ok());
    EXPECT_FALSE(adapter.Delete("oss://bucket/table/").ok());
    EXPECT_FALSE(adapter.Open("oss://bucket/table2/keep").ok());
    EXPECT_FALSE(adapter.Open("oss://bucket/table/../table2/keep").ok());
    EXPECT_FALSE(adapter.Open("oss://other/table/a").ok());
    EXPECT_FALSE(adapter.Create("oss://bucket/table/a?b", false).ok());
    EXPECT_TRUE(adapter.Delete("oss://bucket/table/bucket-0", true).IsNotImplemented());
    EXPECT_TRUE(adapter.Delete("oss://bucket/table/schema/schema-0").IsNotImplemented());
    EXPECT_TRUE(adapter.Create("oss://bucket/table/schema/schema-0", false).status().IsExist());
    EXPECT_TRUE(adapter.Delete("oss://bucket/table/schema/schema-0").IsNotImplemented());
    ASSERT_TRUE(adapter.WriteFile("oss://bucket/table/bucket-0/owned", "owned", false).ok());
    ASSERT_TRUE(adapter.Delete("oss://bucket/table/bucket-0/owned").ok());
    ASSERT_TRUE(adapter.Delete("oss://bucket/table/bucket-0/owned").ok());
    EXPECT_EQ(4, fs->store->objects.size());
    EXPECT_TRUE(fs->store->objects.contains("s3://bucket/table2/keep"));
    EXPECT_TRUE(adapter.AtomicStore("oss://bucket/table/snapshot/a", "x").IsNotImplemented());
    EXPECT_TRUE(adapter.Rename("a", "b").IsNotImplemented());
    fs->store->list_error = Status::IOError("permission denied");
    EXPECT_TRUE(adapter.Exists("oss://bucket/table/missing").status().IsIOError());
}

TEST(DorisPaimonFileSystemTest, FailuresAreStickyAndReleaseWriters) {
    auto fs = std::make_shared<TestObjectStore>();
    DorisPaimonFileSystem adapter(fs, "gs://bucket/table", "s3://bucket/table", nullptr);
    fs->store->close_error = Status::IOError("multipart completion failed");
    EXPECT_TRUE(adapter.WriteFile("gs://bucket/table/a", "data", false).IsIOError());
    EXPECT_EQ(1, fs->store->closed);
    EXPECT_EQ(1, fs->store->destroyed);
    EXPECT_TRUE(fs->store->objects.empty());
    fs->store->close_error = Status::OK();
    fs->store->write_error = Status::MemoryLimitExceeded("injected OOM");
    auto created = adapter.Create("gs://bucket/table/b", false);
    ASSERT_TRUE(created.ok());
    auto out = std::move(created).value();
    EXPECT_TRUE(out->Write("data", 4).status().IsOutOfMemory());
    EXPECT_TRUE(out->Close().IsOutOfMemory());
    EXPECT_TRUE(out->Close().IsOutOfMemory());
    EXPECT_EQ(1, fs->store->closed); // failed write must not complete a partial object
    EXPECT_EQ(2, fs->store->destroyed);
    fs->store->write_error = Status::OK();
    ASSERT_TRUE(adapter.WriteFile("gs://bucket/table/c", "ok", false).ok());
    EXPECT_EQ("ok", fs->store->objects.at("s3://bucket/table/c"));
    {
        auto abandoned = adapter.Create("gs://bucket/table/d", false);
        ASSERT_TRUE(abandoned.ok());
        ASSERT_TRUE(abandoned.value()->Write("data", 4).ok());
    }
    EXPECT_EQ(2, fs->store->closed); // destructor releases, never publishes
    EXPECT_EQ(4, fs->store->destroyed);
    EXPECT_FALSE(fs->store->objects.contains("s3://bucket/table/d"));
}

TEST(DorisPaimonFileSystemTest, PreparedOutputsNeedExplicitHandoff) {
    auto fs = std::make_shared<TestObjectStore>();
    fs->store->objects["s3://bucket/table/existing"] = "keep";
    {
        DorisPaimonFileSystem adapter(fs, "oss://bucket/table", "s3://bucket/table", nullptr);
        // Models an upload completed by PrepareCommit followed by serialization failure.
        ASSERT_TRUE(adapter.WriteFile("oss://bucket/table/unreported", "data", false).ok());
        EXPECT_TRUE(fs->store->objects.contains("s3://bucket/table/unreported"));
    }
    EXPECT_FALSE(fs->store->objects.contains("s3://bucket/table/unreported"));
    EXPECT_TRUE(fs->store->objects.contains("s3://bucket/table/existing"));
    {
        DorisPaimonFileSystem adapter(fs, "oss://bucket/table", "s3://bucket/table", nullptr);
        ASSERT_TRUE(adapter.WriteFile("oss://bucket/table/reported", "data", false).ok());
        adapter.release_owned_files();
        adapter.release_owned_files();
        EXPECT_FALSE(adapter.Create("oss://bucket/table/late", false).ok());
        EXPECT_TRUE(adapter.cleanup_owned_files().ok());
    }
    EXPECT_TRUE(fs->store->objects.contains("s3://bucket/table/reported"));
}

TEST(DorisPaimonFileSystemTest, CleanupContinuesAndRetriesFailedDeletes) {
    auto fs = std::make_shared<TestObjectStore>();
    DorisPaimonFileSystem adapter(fs, "oss://bucket/table", "s3://bucket/table", nullptr);
    ASSERT_TRUE(adapter.WriteFile("oss://bucket/table/a", "a", false).ok());
    ASSERT_TRUE(adapter.WriteFile("oss://bucket/table/b", "b", false).ok());
    fs->store->delete_failure_path = "s3://bucket/table/a";
    EXPECT_TRUE(adapter.cleanup_owned_files().IsIOError());
    EXPECT_TRUE(fs->store->objects.contains("s3://bucket/table/a"));
    EXPECT_FALSE(fs->store->objects.contains("s3://bucket/table/b"));
    fs->store->delete_failure_path.clear();
    EXPECT_TRUE(adapter.cleanup_owned_files().ok());
    EXPECT_TRUE(adapter.cleanup_owned_files().ok());
    EXPECT_TRUE(fs->store->objects.empty());
}
} // namespace doris
