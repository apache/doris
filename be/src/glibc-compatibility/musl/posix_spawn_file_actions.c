#define _GNU_SOURCE
#include <errno.h>
#include <spawn.h>
#include <stdlib.h>
#include <string.h>

#include "fdop.h"

static struct fdop* first_action(const posix_spawn_file_actions_t* file_actions) {
    return (struct fdop*)file_actions->__actions;
}

static void prepend_action(posix_spawn_file_actions_t* file_actions, struct fdop* action) {
    action->next = first_action(file_actions);
    if (action->next) action->next->prev = action;
    action->prev = NULL;
    file_actions->__actions = (struct __spawn_action*)action;
}

int posix_spawn_file_actions_init(posix_spawn_file_actions_t* file_actions) {
    file_actions->__allocated = 0;
    file_actions->__used = 0;
    file_actions->__actions = NULL;
    return 0;
}

int posix_spawn_file_actions_addchdir_np(posix_spawn_file_actions_t* restrict file_actions,
                                         const char* restrict path) {
    struct fdop* action = malloc(sizeof(*action) + strlen(path) + 1);
    if (!action) return ENOMEM;
    action->cmd = FDOP_CHDIR;
    action->fd = -1;
    strcpy(action->path, path);
    prepend_action(file_actions, action);
    return 0;
}

int posix_spawn_file_actions_addclose(posix_spawn_file_actions_t* file_actions, int fd) {
    if (fd < 0) return EBADF;
    struct fdop* action = malloc(sizeof(*action));
    if (!action) return ENOMEM;
    action->cmd = FDOP_CLOSE;
    action->fd = fd;
    prepend_action(file_actions, action);
    return 0;
}

int posix_spawn_file_actions_adddup2(posix_spawn_file_actions_t* file_actions, int source_fd,
                                     int target_fd) {
    if (source_fd < 0 || target_fd < 0) return EBADF;
    struct fdop* action = malloc(sizeof(*action));
    if (!action) return ENOMEM;
    action->cmd = FDOP_DUP2;
    action->srcfd = source_fd;
    action->fd = target_fd;
    prepend_action(file_actions, action);
    return 0;
}

int posix_spawn_file_actions_addfchdir_np(posix_spawn_file_actions_t* file_actions, int fd) {
    if (fd < 0) return EBADF;
    struct fdop* action = malloc(sizeof(*action));
    if (!action) return ENOMEM;
    action->cmd = FDOP_FCHDIR;
    action->fd = fd;
    prepend_action(file_actions, action);
    return 0;
}

int posix_spawn_file_actions_addopen(posix_spawn_file_actions_t* restrict file_actions, int fd,
                                     const char* restrict path, int flags, mode_t mode) {
    if (fd < 0) return EBADF;
    struct fdop* action = malloc(sizeof(*action) + strlen(path) + 1);
    if (!action) return ENOMEM;
    action->cmd = FDOP_OPEN;
    action->fd = fd;
    action->oflag = flags;
    action->mode = mode;
    strcpy(action->path, path);
    prepend_action(file_actions, action);
    return 0;
}

int posix_spawn_file_actions_destroy(posix_spawn_file_actions_t* file_actions) {
    struct fdop* action = first_action(file_actions);
    while (action) {
        struct fdop* next = action->next;
        free(action);
        action = next;
    }
    file_actions->__allocated = 0;
    file_actions->__used = 0;
    file_actions->__actions = NULL;
    return 0;
}
