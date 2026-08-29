#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>

#include "syscall.h"

ssize_t splice(int fd_in, off_t* off_in, int fd_out, off_t* off_out, size_t len,
               unsigned int flags) {
    return syscall(SYS_splice, fd_in, off_in, fd_out, off_out, len, flags);
}
