#pragma once

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "io/fs/file_system.h"
namespace doris::io {

struct EncryptionInfo;

FileSystemSPtr make_file_system(const FileSystemSPtr& inner, EncryptionAlgorithmPB algorithm);

} // namespace doris::io
