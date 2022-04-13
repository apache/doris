#include "filesystem/local_file_system.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <string_view>

#include "filesystem/local_read_stream.h"
#include "filesystem/local_write_stream.h"

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

TEST_F(LocalFileSystemTest, read_file) {
    const std::string content =
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

    int fd = ::open("ut_dir/read_file", O_RDWR | O_CREAT | O_CLOEXEC, 0666);
    ASSERT_EQ(::write(fd, content.c_str(), content.size()), content.size());
    size_t file_size = content.size();

    constexpr size_t buffer_size = 64;
    LocalReadStream istream(fd, file_size, buffer_size);

    char buf1[BUFSIZ];
    size_t read_n1 = 0;

    auto s = istream.read(buf1, file_size, &read_n1);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(istream.eof());
    ASSERT_EQ(std::string_view(buf1, read_n1), content);

    size_t off = 0;
    s = istream.seek(off);
    ASSERT_TRUE(s.ok());
    istream.read(buf1, 16, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 16));
    off += 16;
    istream.read(buf1, 32, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 32));
    off += 32;
    istream.read(buf1, 128, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 128));

    off = 60;
    s = istream.seek(off);
    ASSERT_TRUE(s.ok());
    istream.read(buf1, 16, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 16));
    off += 16;
    istream.read(buf1, 64, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 64));
    off += 64;
    istream.read(buf1, 128, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 128));

    off = file_size - 108;
    s = istream.seek(off);
    ASSERT_TRUE(s.ok());
    istream.read(buf1, 16, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 16));
    off += 16;
    istream.read(buf1, 32, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 32));
    off += 32;
    istream.read(buf1, 64, &read_n1);
    ASSERT_EQ(std::string_view(buf1, read_n1), std::string_view(content.data() + off, 60));

    ASSERT_TRUE(istream.close().ok());

    ::close(fd);
}

TEST_F(LocalFileSystemTest, write_file) {
    const std::string content =
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

    int fd = ::open("ut_dir/write_file", O_RDWR | O_CREAT | O_CLOEXEC, 0666);

    constexpr size_t buffer_size = 64;
    LocalWriteStream ostream(fd, buffer_size);

    size_t off = 0;
    ASSERT_TRUE(ostream.write(content.data(), 16).ok());
    off += 16;
    ASSERT_TRUE(ostream.write(content.data() + off, 32).ok());
    off += 32;
    ASSERT_TRUE(ostream.write(content.data() + off, 64).ok());
    off += 64;
    ASSERT_TRUE(ostream.write(content.data() + off, 64).ok());
    off += 64;
    ASSERT_TRUE(ostream.write(content.data() + off, 128).ok());
    off += 128;
    ASSERT_TRUE(ostream.sync().ok());

    char buf1[BUFSIZ];
    auto res = ::pread(fd, buf1, BUFSIZ, 0);
    ASSERT_EQ(std::string_view(buf1, res), std::string_view(content.data(), off));

    ASSERT_TRUE(ostream.write(content.data() + off, content.size() - off).ok());
    ASSERT_TRUE(ostream.sync().ok());

    res = ::pread(fd, buf1, BUFSIZ, 0);
    ASSERT_EQ(std::string_view(buf1, res), content);

    ASSERT_TRUE(ostream.close().ok());
}

} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
