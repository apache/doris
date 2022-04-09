#include "filesystem/local_file_system.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace doris {

class LocalFileSystemTest : public testing::Test {
public:
    LocalFileSystemTest() = default;
    ~LocalFileSystemTest() override = default;
};

TEST_F(LocalFileSystemTest, file_system_api) {
    namespace fs = std::filesystem;
    fs::create_directories("ut_dir/local_fs");
    fs::create_directories("ut_dir/local_fs/dir");
    system("touch ut_dir/local_fs/file");
    system("touch ut_dir/local_fs/dir/a");
    system("touch ut_dir/local_fs/dir/b");
    system("touch ut_dir/local_fs/dir/c");

    auto local_fs = LocalFileSystem("ut_dir/local_fs");
    bool res;

    local_fs.exists("dir", &res);
    ASSERT_TRUE(res);
    local_fs.exists("abc", &res);
    ASSERT_FALSE(res);

    local_fs.is_directory("dir", &res);
    ASSERT_TRUE(res);
    local_fs.is_directory("file", &res);
    ASSERT_FALSE(res);
    local_fs.is_directory("abc", &res);
    ASSERT_FALSE(res);

    local_fs.is_file("dir", &res);
    ASSERT_FALSE(res);
    local_fs.is_file("file", &res);
    ASSERT_TRUE(res);
    local_fs.is_file("abc", &res);
    ASSERT_FALSE(res);

    std::vector<FileStat> files;
    local_fs.list("dir", &files);
    ASSERT_EQ(files.size(), 3);
    auto s = local_fs.list("file", &files);
    ASSERT_TRUE(!s.ok());

    s = local_fs.delete_file("file");
    ASSERT_TRUE(s.ok());
    s = local_fs.delete_file("dir");
    ASSERT_TRUE(!s.ok());
    s = local_fs.delete_file("abc");
    ASSERT_TRUE(s.ok());

    s = local_fs.delete_directory("dir/a");
    ASSERT_TRUE(!s.ok());
    s = local_fs.delete_directory("dir");
    ASSERT_TRUE(s.ok());
    s = local_fs.delete_directory("abc");
    ASSERT_TRUE(s.ok());

    s = local_fs.create_directory("dir/dir");
    ASSERT_TRUE(s.ok());
    s = local_fs.create_directory("dir");
    ASSERT_TRUE(!s.ok());
}

} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
