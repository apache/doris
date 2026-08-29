#include <spawn.h>

int __execvpe(const char* file, char* const argv[], char* const envp[]);

int __posix_spawnx(pid_t* restrict result, const char* restrict path,
                   int (*exec)(const char*, char* const*, char* const*),
                   const posix_spawn_file_actions_t* file_actions,
                   const posix_spawnattr_t* restrict attr, char* const argv[restrict],
                   char* const envp[restrict]);

int posix_spawnp(pid_t* restrict result, const char* restrict file,
                 const posix_spawn_file_actions_t* file_actions,
                 const posix_spawnattr_t* restrict attr, char* const argv[restrict],
                 char* const envp[restrict]) {
    return __posix_spawnx(result, file, __execvpe, file_actions, attr, argv, envp);
}
