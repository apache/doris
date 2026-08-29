#define _DEFAULT_SOURCE
#include <sys/uio.h>
#include <unistd.h>

#include "syscall.h"

ssize_t preadv(int fd, const struct iovec* iov, int count, off_t offset) {
    return syscall(SYS_preadv, fd, iov, count, (long)(offset), (long)(offset >> 32));
}
