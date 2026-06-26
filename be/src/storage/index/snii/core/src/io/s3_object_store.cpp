#include "snii/io/s3_object_store.h"

// The whole implementation is compiled only when the S3 backend is enabled.
// Without SNII_WITH_S3 this file is an empty translation unit and pulls in no
// aws-sdk headers, keeping core aws-free by default.
#ifdef SNII_WITH_S3

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <atomic>
#include <future>
#include <mutex>
#include <sstream>
#include <utility>

namespace snii::io {
namespace {

// Refcounted process-wide InitAPI/ShutdownAPI control, shared by AwsApiGuard.
std::mutex g_api_mu;
int g_api_refcount = 0;
Aws::SDKOptions g_api_options;

void api_acquire() {
    std::lock_guard<std::mutex> lock(g_api_mu);
    if (g_api_refcount == 0) {
        Aws::InitAPI(g_api_options);
    }
    ++g_api_refcount;
}

void api_release() {
    std::lock_guard<std::mutex> lock(g_api_mu);
    if (g_api_refcount > 0) {
        --g_api_refcount;
        if (g_api_refcount == 0) {
            Aws::ShutdownAPI(g_api_options);
        }
    }
}

// Builds a virtual-hosted-addressing S3 client for an OSS-compatible endpoint.
// OSS rejects path-style addressing (SecondLevelDomainForbidden), so virtual
// addressing is mandatory; payload signing is disabled (Never).
std::shared_ptr<Aws::S3::S3Client> make_client(const S3Config& cfg) {
    Aws::Auth::AWSCredentials creds(Aws::String(cfg.ak.c_str()), Aws::String(cfg.sk.c_str()));
    Aws::Client::ClientConfigurationInitValues init;
    init.shouldDisableIMDS = true;
    Aws::Client::ClientConfiguration client_cfg(init);
    client_cfg.endpointOverride = Aws::String(cfg.endpoint.c_str());
    client_cfg.region = Aws::String(cfg.region.c_str());
    client_cfg.connectTimeoutMs = cfg.connect_timeout_ms;
    client_cfg.requestTimeoutMs = cfg.request_timeout_ms;
    client_cfg.httpRequestTimeoutMs = cfg.http_request_timeout_ms;
    return std::make_shared<Aws::S3::S3Client>(
            creds, client_cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /*useVirtualAddressing=*/true);
}

std::string join_key(const std::string& prefix, const std::string& key) {
    if (prefix.empty()) return key;
    return prefix + "/" + key;
}

} // namespace

AwsApiGuard::AwsApiGuard() {
    api_acquire();
}
AwsApiGuard::~AwsApiGuard() {
    api_release();
}

// ---------------------------------------------------------------------------
// S3FileReader
// ---------------------------------------------------------------------------

S3FileReader::~S3FileReader() = default;

S3FileReader::S3FileReader(S3FileReader&&) noexcept = default;
S3FileReader& S3FileReader::operator=(S3FileReader&&) noexcept = default;

Status S3FileReader::open(const S3Config& cfg, const std::string& key, S3FileReader* out) {
    if (out == nullptr) return Status::InvalidArgument("S3FileReader::open: null out");
    out->client_ = make_client(cfg);
    out->bucket_ = cfg.bucket;
    out->object_key_ = join_key(cfg.prefix, key);

    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(Aws::String(out->bucket_.c_str()));
    req.SetKey(Aws::String(out->object_key_.c_str()));
    auto outcome = out->client_->HeadObject(req);
    if (!outcome.IsSuccess()) {
        return Status::IoError("HeadObject(" + out->object_key_ +
                               "): " + outcome.GetError().GetMessage().c_str());
    }
    out->size_ = static_cast<uint64_t>(outcome.GetResult().GetContentLength());
    return Status::OK();
}

Status S3FileReader::read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) {
    if (client_ == nullptr) return Status::IoError("read_at on unopened S3 object");
    if (out == nullptr) return Status::InvalidArgument("read_at: null out");
    // Non-wrapping bounds check (offset+len could overflow uint64 on a corrupt arg).
    if (offset > size_ || len > size_ - offset) {
        return Status::Corruption("read_at past end of object");
    }
    out->resize(len);
    if (len == 0) return Status::OK();

    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(Aws::String(bucket_.c_str()));
    req.SetKey(Aws::String(object_key_.c_str()));
    std::ostringstream range;
    range << "bytes=" << offset << "-" << (offset + len - 1);
    req.SetRange(Aws::String(range.str().c_str()));

    auto outcome = client_->GetObject(req);
    if (!outcome.IsSuccess()) {
        return Status::IoError("GetObject(" + object_key_ +
                               "): " + outcome.GetError().GetMessage().c_str());
    }
    auto& body = outcome.GetResult().GetBody();
    body.read(reinterpret_cast<char*>(out->data()), static_cast<std::streamsize>(len));
    const std::streamsize got = body.gcount();
    if (static_cast<size_t>(got) != len) {
        return Status::Corruption("GetObject returned fewer bytes than requested");
    }
    return Status::OK();
}

Status S3FileReader::read_batch(const std::vector<Range>& ranges,
                                std::vector<std::vector<uint8_t>>* outs) {
    if (outs == nullptr) return Status::InvalidArgument("read_batch: null outs");
    outs->resize(ranges.size());
    if (ranges.empty()) return Status::OK();
    // Issue GETs concurrently in bounded waves; aws S3Client is safe for parallel
    // requests and each range writes a distinct output buffer.
    constexpr size_t kMaxConcurrent = 16;
    Status first_err;
    for (size_t base = 0; base < ranges.size(); base += kMaxConcurrent) {
        const size_t end = std::min(base + kMaxConcurrent, ranges.size());
        std::vector<std::future<Status>> futs;
        for (size_t i = base; i < end; ++i) {
            futs.push_back(std::async(std::launch::async, [this, &ranges, outs, i]() {
                return read_at(ranges[i].offset, ranges[i].len, &(*outs)[i]);
            }));
        }
        for (auto& f : futs) {
            const Status s = f.get();
            if (!s.ok() && first_err.ok()) first_err = s;
        }
    }
    return first_err;
}

// ---------------------------------------------------------------------------
// S3FileWriter
// ---------------------------------------------------------------------------

S3FileWriter::~S3FileWriter() = default;

S3FileWriter::S3FileWriter(S3FileWriter&&) noexcept = default;
S3FileWriter& S3FileWriter::operator=(S3FileWriter&&) noexcept = default;

Status S3FileWriter::open(const S3Config& cfg, const std::string& key) {
    client_ = make_client(cfg);
    bucket_ = cfg.bucket;
    object_key_ = join_key(cfg.prefix, key);
    buffer_.clear();
    bytes_written_ = 0;
    finalized_ = false;
    return Status::OK();
}

Status S3FileWriter::append(Slice data) {
    if (client_ == nullptr) return Status::IoError("append on unopened S3 writer");
    if (finalized_) return Status::IoError("append after finalize");
    buffer_.insert(buffer_.end(), data.data(), data.data() + data.size());
    bytes_written_ += data.size();
    return Status::OK();
}

Status S3FileWriter::finalize() {
    if (client_ == nullptr) return Status::IoError("finalize on unopened S3 writer");
    if (finalized_) return Status::IoError("finalize called twice");

    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(Aws::String(bucket_.c_str()));
    req.SetKey(Aws::String(object_key_.c_str()));
    auto stream = Aws::MakeShared<Aws::StringStream>("S3FileWriter");
    stream->write(reinterpret_cast<const char*>(buffer_.data()),
                  static_cast<std::streamsize>(buffer_.size()));
    req.SetBody(stream);
    req.SetContentLength(static_cast<long long>(buffer_.size()));

    auto outcome = client_->PutObject(req);
    if (!outcome.IsSuccess()) {
        return Status::IoError("PutObject(" + object_key_ +
                               "): " + outcome.GetError().GetMessage().c_str());
    }
    finalized_ = true;
    return Status::OK();
}

} // namespace snii::io

#endif // SNII_WITH_S3
