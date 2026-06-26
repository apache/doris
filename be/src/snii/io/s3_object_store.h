#pragma once

// S3 / OSS object-storage backend for snii::io.
//
// ISOLATION: the ENTIRE body of this header (and its .cpp) is guarded by
// SNII_WITH_S3. When the option is OFF the translation unit compiles to nothing
// and pulls in NO aws-sdk headers, so core stays free of any aws dependency by
// default. Only when CMake is configured with -DSNII_WITH_S3=ON is the macro
// defined and aws linked.
#ifdef SNII_WITH_S3

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/io/file_reader.h"
#include "snii/io/file_writer.h"

// Forward declarations only -- aws types are pimpl'd in the .cpp so that this
// header never leaks aws-sdk includes to its consumers.
namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace snii::io {

// Connection / addressing parameters for an S3-compatible endpoint (tested
// against Aliyun OSS, which requires virtual-hosted addressing).
struct S3Config {
    std::string endpoint; // e.g. "oss-cn-hongkong.aliyuncs.com"
    std::string region;   // e.g. "cn-hongkong"
    std::string bucket;   // e.g. "doris-community-test"
    std::string prefix;   // object key prefix (no trailing slash required)
    std::string ak;       // access key id
    std::string sk;       // secret access key
    long connect_timeout_ms = 10000;
    long request_timeout_ms = 180000;
    long http_request_timeout_ms = 180000;
};

// Process-wide aws InitAPI / ShutdownAPI lifecycle guard.
//
// aws-sdk-cpp requires Aws::InitAPI to be called exactly once before any client
// is used and Aws::ShutdownAPI once at teardown. Construct a single
// AwsApiGuard (e.g. on the stack of main, or as a static) that lives for the
// whole duration during which S3FileReader / S3FileWriter are used. The guard is
// reference counted, so nested guards are safe; the underlying InitAPI runs only
// for the first live instance and ShutdownAPI when the last one is destroyed.
class AwsApiGuard {
public:
    AwsApiGuard();
    ~AwsApiGuard();

    AwsApiGuard(const AwsApiGuard&) = delete;
    AwsApiGuard& operator=(const AwsApiGuard&) = delete;
};

// Read-only FileReader backed by an S3/OSS object. Range reads use a ranged
// GetObject; size() is the object length cached from a HeadObject at open().
class S3FileReader : public FileReader {
public:
    S3FileReader() = default;
    ~S3FileReader() override;

    S3FileReader(const S3FileReader&) = delete;
    S3FileReader& operator=(const S3FileReader&) = delete;
    S3FileReader(S3FileReader&&) noexcept;
    S3FileReader& operator=(S3FileReader&&) noexcept;

    // Opens the object (prefix + "/" + key) and caches its size via HeadObject.
    static Status open(const S3Config& cfg, const std::string& key, S3FileReader* out);

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override;
    // Concurrent batch: issues the ranges' GetObjects in parallel (bounded), so a
    // planned read round costs ~one round-trip instead of the sum of all GETs.
    Status read_batch(const std::vector<Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override;
    uint64_t size() const override { return size_; }

private:
    std::shared_ptr<Aws::S3::S3Client> client_;
    std::string bucket_;
    std::string object_key_; // full key (prefix + "/" + key)
    uint64_t size_ = 0;
};

// Append-only FileWriter backed by an S3/OSS object. Appends are buffered in
// memory; finalize() flushes the whole buffer in a single PutObject. Multipart
// upload is a future optimization.
class S3FileWriter : public FileWriter {
public:
    S3FileWriter() = default;
    ~S3FileWriter() override;

    S3FileWriter(const S3FileWriter&) = delete;
    S3FileWriter& operator=(const S3FileWriter&) = delete;
    S3FileWriter(S3FileWriter&&) noexcept;
    S3FileWriter& operator=(S3FileWriter&&) noexcept;

    // Opens a writer targeting object (prefix + "/" + key).
    Status open(const S3Config& cfg, const std::string& key);

    Status append(Slice data) override;
    Status finalize() override;
    uint64_t bytes_written() const override { return bytes_written_; }

private:
    std::shared_ptr<Aws::S3::S3Client> client_;
    std::string bucket_;
    std::string object_key_; // full key (prefix + "/" + key)
    std::vector<uint8_t> buffer_;
    uint64_t bytes_written_ = 0;
    bool finalized_ = false;
};

} // namespace snii::io

#endif // SNII_WITH_S3
