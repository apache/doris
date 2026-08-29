#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char* strchrnul_compat(const char* str, int ch) {
    const char* found = strchr(str, ch);
    return (char*)(found ? found : str + strlen(str));
}

int __execvpe(const char* file, char* const argv[], char* const envp[]) {
    const char* path = getenv("PATH");
    int seen_eacces = 0;

    errno = ENOENT;
    if (!*file) return -1;
    if (strchr(file, '/')) return execve(file, argv, envp);
    if (!path) path = "/usr/local/bin:/bin:/usr/bin";

    size_t file_len = strnlen(file, NAME_MAX + 1);
    if (file_len > NAME_MAX) {
        errno = ENAMETOOLONG;
        return -1;
    }
    size_t path_len = strnlen(path, PATH_MAX - 1) + 1;

    const char* cursor;
    const char* end;
    for (cursor = path;; cursor = end) {
        char candidate[path_len + file_len + 1];
        end = strchrnul_compat(cursor, ':');
        if (end - cursor >= path_len) {
            if (!*end++) break;
            continue;
        }
        memcpy(candidate, cursor, end - cursor);
        candidate[end - cursor] = '/';
        memcpy(candidate + (end - cursor) + (end > cursor), file, file_len + 1);
        execve(candidate, argv, envp);
        switch (errno) {
        case EACCES:
            seen_eacces = 1;
            // Fall through.
        case ENOENT:
        case ENOTDIR:
            break;
        default:
            return -1;
        }
        if (!*end++) break;
    }
    if (seen_eacces) errno = EACCES;
    return -1;
}
