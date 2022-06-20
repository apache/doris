#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>

namespace doris {

// A non-copying iostream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
public:
    StringViewStream(const void* buf, int64_t nbytes)
            : Aws::Utils::Stream::PreallocatedStreamBuf(
                      reinterpret_cast<unsigned char*>(const_cast<void*>(buf)),
                      static_cast<size_t>(nbytes)),
              std::iostream(this) {}
};

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read directly into our preallocated buffer instead.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
inline Aws::IOStreamFactory AwsWriteableStreamFactory(void* buf, int64_t nbytes) {
    return [=]() { return Aws::New<StringViewStream>("", buf, nbytes); };
}

} // namespace doris
