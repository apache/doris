#pragma once

#include <sys/types.h>

#define FDOP_CLOSE 1
#define FDOP_DUP2 2
#define FDOP_OPEN 3
#define FDOP_CHDIR 4
#define FDOP_FCHDIR 5

struct fdop {
    struct fdop* next;
    struct fdop* prev;
    int cmd;
    int fd;
    int srcfd;
    int oflag;
    mode_t mode;
    char path[];
};
