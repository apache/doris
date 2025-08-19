#include "io/fs/encrypted_fs_factory.h"

#include <gen_cpp/olap_file.pb.h>

#include "common/exception.h"
#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris::io {

FileSystemSPtr make_file_system(const FileSystemSPtr& inner, EncryptionAlgorithmPB algorithm) {
    if (algorithm == EncryptionAlgorithmPB::PLAINTEXT) {
        return inner;
    }
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Current version does not support TDE");
}

} // namespace doris::io
