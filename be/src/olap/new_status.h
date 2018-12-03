// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DORIS_BE_SRC_OLAP_STATUS_H
#define DORIS_BE_SRC_OLAP_STATUS_H

#include <string>
#include "util/slice.h"

namespace doris {

#define WARN_AND_RETURN(status) do { \
    const NewStatus& s = (status);        \
    LOG(WARNING) << s.ToString(); \
    return s; \
} while (0);

class NewStatus {
public:
    // Create a success status.
    NewStatus() : _code(kOk), _posix_code(0), _state(nullptr) {}
    ~NewStatus() { delete[] _state; }

    // Copy the specified status.
    NewStatus(const NewStatus& s);
    void operator=(const NewStatus& s);

    // Return a success status.
    static NewStatus OK() { return NewStatus(); }

    // Return error status of an appropriate type.
    static NewStatus NotFound(const Slice& msg, const Slice& msg2 = Slice(),
                           int32_t posix_code = -1) {
        return NewStatus(kNotFound, msg, msg2, posix_code);
    }
    static NewStatus Corruption(const Slice& msg, const Slice& msg2 = Slice(),
                             int32_t posix_code = -1) {
        return NewStatus(kCorruption, msg, msg2, posix_code);
    }
    static NewStatus NotSupported(const Slice& msg, const Slice& msg2 = Slice(),
                               int32_t posix_code = -1) {
        return NewStatus(kNotSupported, msg, msg2, posix_code);
    }
    static NewStatus InvalidArgument(const Slice& msg, const Slice& msg2 = Slice(),
                                  int32_t posix_code = -1) {
        return NewStatus(kInvalidArgument, msg, msg2, posix_code);
    }
    static NewStatus AlreadyExist(const Slice& msg, const Slice& msg2 = Slice(),
                                  int32_t posix_code = -1) {
        return NewStatus(kAlreadyExist, msg, msg2, posix_code);
    }
    static NewStatus NoSpace(const Slice& msg, const Slice& msg2 = Slice(),
                          int32_t posix_code = -1) {
        return NewStatus(kNoSpace, msg, msg2, posix_code);
    }
    static NewStatus EndOfFile(const Slice& msg, const Slice& msg2 = Slice(),
                            int32_t posix_code = -1) {
        return NewStatus(kEndOfFile, msg, msg2, posix_code);
    }
    static NewStatus DiskFailure(const Slice& msg, const Slice& msg2 = Slice(),
                              int32_t posix_code = -1) {
        return NewStatus(kDiskFailure, msg, msg2, posix_code);
    }
    static NewStatus IOError(const Slice& msg, const Slice& msg2 = Slice(),
                          int32_t posix_code = -1) {
        return NewStatus(kIOError, msg, msg2, posix_code);
    }
    static NewStatus TimedOut(const Slice& msg, const Slice& msg2 = Slice(),
                           int32_t posix_code = -1) {
        return NewStatus(kTimedOut, msg, msg2, posix_code);
    }
    static NewStatus MemoryLimitExceeded(const Slice& msg, const Slice& msg2 = Slice(),
                                      int32_t posix_code = -1) {
        return NewStatus(kMemoryLimitExceeded, msg, msg2, posix_code);
    }
    static NewStatus DeadLock(const Slice& msg, const Slice& msg2 = Slice(),
                           int32_t posix_code = -1) {
        return NewStatus(kDeadLock, msg, msg2, posix_code);
    }

    // Returns true iff the status indicates success.
    bool ok() const { return code() == kOk; }

    // Returns true iff the status indicates a NotFound error.
    bool IsNotFound() const { return code() == kNotFound; }

    // Returns true iff the status indicates a Corruption error.
    bool IsCorruption() const { return code() == kCorruption; }

    // Returns true iff the status indicates a NotSupportedError.
    bool IsNotSupported() const { return code() == kNotSupported; }

    // Returns true iff the status indicates an InvalidArgument.
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    // Returns true iff the status indicates an AlreadyExist.
    bool IsAlreadyExist() const { return code() == kAlreadyExist; }

    // Returns true iff the status indicates an NoSpace Error.
    bool IsNoSpace() const { return code() == kNoSpace; }

    // Returns true iff the status indicates an end of file.
    bool IsEndOfFile() const { return code() == kEndOfFile; }

    // Returns true iff the status indicates an IOError.
    bool IsDiskFailure() const { return code() == kDiskFailure; }

    // Returns true iff the status indicates an IOError.
    bool IsIOError() const { return code() == kIOError; }

    // Returns true iff the status indicates timed out.
    bool IsTimedOut() const { return code() == kTimedOut; }

    // Returns true iff the status indicates a memory limit error.
    bool IsMemoryLimitExceeded() const { return code() == kMemoryLimitExceeded; }

    // Returns true iff the status indicates a DeadLock.
    bool IsDeadLock() const { return code() == kDeadLock; }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string ToString() const;

    // return A string representation of the status code, without the message
    // text or POSIX code information
    std::string CodeAsString() const;

    // return The Posix code associated with this Status Object
    inline int32_t posix_code() const { return _posix_code; }

private:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kAlreadyExist = 5,
        kNoSpace = 6,
        kEndOfFile = 7, 
        kIOError = 8,
        kDiskFailure = 9,
        kTimedOut = 10,
        kMemoryLimitExceeded = 11,
        kDeadLock = 12
    };

    Code _code;
    Code code() const { return _code; }

    // The POSIX code accociated with the NewStatus object, 
    // if _posix_code == -1, indicates no posix_code.
    int32_t _posix_code;

    // OK status has a nullptr _state.  Otherwise, _state is a new[] array
    // of the following form:
    //    _state[0..3] == length of message
    //    _state[4..]  == message
    const char* _state;

    NewStatus(Code code, const Slice& msg, const Slice& msg2, int32_t posix_code);
    static const char* CopyState(const char* s);
};

inline NewStatus::NewStatus(const NewStatus& s)
        : _code(s._code), _posix_code(s._posix_code) {
    _state = (s._state == nullptr) ? nullptr : CopyState(s._state);
}

inline void NewStatus::operator=(const NewStatus& s) {
    // The following condition catches both aliasing (when this == &s),
    // and the common case where both s and *this are ok.
    if (_state != s._state) {
        _code = s._code;
        _posix_code = s._posix_code;
        delete[] _state;
        _state = (s._state == nullptr) ? nullptr : CopyState(s._state);
    }
}

std::string ErrnoToString(int err);
NewStatus IOError(const std::string& context, int err);


}  // namespace doris

#endif  // DORIS_BE_SRC_OLAP_STATUS_H
