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

#pragma once

#include <boost/exception/all.hpp>
#include <boost/stacktrace.hpp>
#include <cerrno>
#include <memory>
#include <stdexcept>
#include <vector>

#include "common/status.h"

using stacktrace = boost::error_info<struct tag_stacktrace, boost::stacktrace::stacktrace>;

namespace doris::vectorized {

class AbstractException : public std::exception
/// This is the base class for all exceptions defined
/// in the Poco class library.
{
public:
    AbstractException(const std::string& msg, int code = 0);
    /// Creates an exception.

    AbstractException(const std::string& msg, const std::string& arg, int code = 0);
    /// Creates an exception.

    AbstractException(const std::string& msg, const AbstractException& nested, int code = 0);
    /// Creates an exception and stores a clone
    /// of the nested exception.

    AbstractException(const AbstractException& exc);
    /// Copy constructor.

    ~AbstractException() throw();
    /// Destroys the exception and deletes the nested exception.

    AbstractException& operator=(const AbstractException& exc);
    /// Assignment operator.

    virtual const char* name() const throw();
    /// Returns a static string describing the exception.

    virtual const char* className() const throw();
    /// Returns the name of the exception class.

    virtual const char* what() const throw();
    /// Returns a static string describing the exception.
    ///
    /// Same as name(), but for compatibility with std::exception.

    const AbstractException* nested() const;
    /// Returns a pointer to the nested exception, or
    /// null if no nested exception exists.

    const std::string& message() const;
    /// Returns the message text.

    int code() const;
    /// Returns the exception code if defined.

    virtual std::string displayText() const;
    /// Returns a string consisting of the
    /// message name and the message text.

    virtual AbstractException* clone() const;
    /// Creates an exact copy of the exception.
    ///
    /// The copy can later be thrown again by
    /// invoking rethrow() on it.

    virtual void rethrow() const;
    /// (Re)Throws the exception.
    ///
    /// This is useful for temporarily storing a
    /// copy of an exception (see clone()), then
    /// throwing it again.

protected:
    AbstractException(int code = 0);
    /// Standard constructor.

    void message(const std::string& msg);
    /// Sets the message for the exception.

    void extendedMessage(const std::string& arg);
    /// Sets the extended message for the exception.

private:
    std::string _msg;
    AbstractException* _pNested;
    int _code;
};

//
// inlines
//
inline const AbstractException* AbstractException::nested() const {
    return _pNested;
}

inline const std::string& AbstractException::message() const {
    return _msg;
}

inline void AbstractException::message(const std::string& msg) {
    _msg = msg;
}

inline int AbstractException::code() const {
    return _code;
}

//
// Macros for quickly declaring and implementing exception classes.
// Unfortunately, we cannot use a template here because character
// pointers (which we need for specifying the exception name)
// are not allowed as template arguments.
//
#define DORIS_DECLARE_EXCEPTION_CODE(API, CLS, BASE, CODE)                          \
    class CLS : public BASE {                                                       \
    public:                                                                         \
        CLS(int code = CODE);                                                       \
        CLS(const std::string& msg, int code = CODE);                               \
        CLS(const std::string& msg, const std::string& arg, int code = CODE);       \
        CLS(const std::string& msg, const AbstractException& exc, int code = CODE); \
        CLS(const CLS& exc);                                                        \
        ~CLS() throw();                                                             \
        CLS& operator=(const CLS& exc);                                             \
        const char* name() const throw();                                           \
        const char* className() const throw();                                      \
        AbstractException* clone() const;                                           \
        void rethrow() const;                                                       \
    };

#define DORIS_DECLARE_EXCEPTION(API, CLS, BASE) POCO_DECLARE_EXCEPTION_CODE(API, CLS, BASE, 0)

#define DORIS_IMPLEMENT_EXCEPTION(CLS, BASE, NAME)                                               \
    CLS::CLS(int code) : BASE(code) {}                                                           \
    CLS::CLS(const std::string& msg, int code) : BASE(msg, code) {}                              \
    CLS::CLS(const std::string& msg, const std::string& arg, int code) : BASE(msg, arg, code) {} \
    CLS::CLS(const std::string& msg, const AbstractException& exc, int code)                     \
            : BASE(msg, exc, code) {}                                                            \
    CLS::CLS(const CLS& exc) : BASE(exc) {}                                                      \
    CLS::~CLS() throw() {}                                                                       \
    CLS& CLS::operator=(const CLS& exc) {                                                        \
        BASE::operator=(exc);                                                                    \
        return *this;                                                                            \
    }                                                                                            \
    const char* CLS::name() const throw() { return NAME; }                                       \
    const char* CLS::className() const throw() { return typeid(*this).name(); }                  \
    AbstractException* CLS::clone() const { return new CLS(*this); }                             \
    void CLS::rethrow() const { throw *this; }

class Exception : public AbstractException {
public:
    Exception() : trace(boost::stacktrace::stacktrace()) {} /// For deferred initialization.
    Exception(const std::string& msg, int code)
            : AbstractException(msg, code), trace(boost::stacktrace::stacktrace()) {}
    Exception(const std::string& msg, const Exception& nested_exception, int code)
            : AbstractException(msg, nested_exception, code), trace(nested_exception.trace) {}

    enum CreateFromPocoTag { CreateFromPoco };
    Exception(CreateFromPocoTag, const AbstractException& exc)
            : AbstractException(exc.displayText(), TStatusCode::VEC_EXCEPTION),
              trace(boost::stacktrace::stacktrace()) {}

    Exception* clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }
    const char* name() const throw() override { return "doris::vectorized::Exception"; }
    const char* what() const throw() override { return message().data(); }

    /// Add something to the existing message.
    void addMessage(const std::string& arg) { extendedMessage(arg); }

    //const StackTrace& getStackTrace() const { return trace; }
    const stacktrace& getStackTrace() const { return trace; }

private:
    //StackTrace trace;
    stacktrace trace;

    const char* className() const throw() override { return "doris::vectorized::Exception"; }
};

/// Contains an additional member `saved_errno`. See the throwFromErrno function.
class ErrnoException : public Exception {
public:
    ErrnoException(const std::string& msg, int code, int saved_errno_,
                   const std::optional<std::string>& path_ = {})
            : Exception(msg, code), saved_errno(saved_errno_), path(path_) {}

    ErrnoException* clone() const override { return new ErrnoException(*this); }
    void rethrow() const override { throw *this; }

    int getErrno() const { return saved_errno; }
    const std::optional<std::string> getPath() const { return path; }

private:
    int saved_errno;
    std::optional<std::string> path;

    const char* name() const throw() override { return "doris::vectorized::ErrnoException"; }
    const char* className() const throw() override { return "doris::vectorized::ErrnoException"; }
};

using Exceptions = std::vector<std::exception_ptr>;

std::string errnoToString(int code, int the_errno = errno);
[[noreturn]] void throwFromErrno(const std::string& s, int code, int the_errno = errno);
[[noreturn]] void throwFromErrnoWithPath(const std::string& s, const std::string& path, int code,
                                         int the_errno = errno);

/** Try to write an exception to the log (and forget about it).
  * Can be used in destructors in the catch-all block.
  */
void tryLogCurrentException(const char* log_name, const std::string& start_of_message = "");

/** Prints current exception in canonical format.
  * with_stacktrace - prints stack trace for doris::vectorized::Exception.
  * check_embedded_stacktrace - if doris::vectorized::Exception has embedded stacktrace then
  *  only this stack trace will be printed.
  */
std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace = false,
                                       bool with_extra_info = true);

int getCurrentExceptionCode();

/// An execution status of any piece of code, contains return code and optional error
struct ExecutionStatus {
    int code = 0;
    std::string message;

    ExecutionStatus() = default;

    explicit ExecutionStatus(int return_code, const std::string& exception_message = "")
            : code(return_code), message(exception_message) {}

    static ExecutionStatus fromCurrentException(const std::string& start_of_message = "");

    std::string serializeText() const;

    void deserializeText(const std::string& data);

    bool tryDeserializeText(const std::string& data);
};

void tryLogException(std::exception_ptr e, const char* log_name,
                     const std::string& start_of_message = "");

std::string getExceptionMessage(const Exception& e, bool with_stacktrace,
                                bool check_embedded_stacktrace = false);
std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace);

void rethrowFirstException(const Exceptions& exceptions);

template <typename T>
std::enable_if_t<std::is_pointer_v<T>, T> exception_cast(std::exception_ptr e) {
    try {
        std::rethrow_exception(std::move(e));
    } catch (std::remove_pointer_t<T>& concrete) {
        return &concrete;
    } catch (...) {
        return nullptr;
    }
}

} // namespace doris::vectorized
