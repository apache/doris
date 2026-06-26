#pragma once

#include <string>
#include <utility>

namespace snii {

enum class StatusCode {
    kOk,
    kCorruption,
    kNotFound,
    kInvalidArgument,
    kIoError,
    kUnsupported,
    kInternal,
};

// Lightweight error type: success is kOk with no message; failure carries a code + human-readable message.
// Always return Status across API boundaries; silent failures are not allowed.
class Status {
public:
    Status() = default;

    static Status OK() { return Status(); }
    static Status Corruption(std::string m) {
        return Status(StatusCode::kCorruption, std::move(m));
    }
    static Status NotFound(std::string m) { return Status(StatusCode::kNotFound, std::move(m)); }
    static Status InvalidArgument(std::string m) {
        return Status(StatusCode::kInvalidArgument, std::move(m));
    }
    static Status IoError(std::string m) { return Status(StatusCode::kIoError, std::move(m)); }
    static Status Unsupported(std::string m) {
        return Status(StatusCode::kUnsupported, std::move(m));
    }
    static Status Internal(std::string m) { return Status(StatusCode::kInternal, std::move(m)); }

    bool ok() const { return code_ == StatusCode::kOk; }
    StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }
    std::string to_string() const;

private:
    Status(StatusCode c, std::string m) : code_(c), message_(std::move(m)) {}

    StatusCode code_ = StatusCode::kOk;
    std::string message_;
};

} // namespace snii

// Short-circuit return for expressions returning Status (propagate errors upward).
#define SNII_RETURN_IF_ERROR(expr)  \
    do {                            \
        ::snii::Status _s = (expr); \
        if (!_s.ok()) return _s;    \
    } while (0)
