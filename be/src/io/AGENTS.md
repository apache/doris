# BE IO — Review Guide

- [ ] Public `FileSystem` operations use `FILESYSTEM_M` (inline on pthreads, `AsyncIO::run_task` on bthreads)?
- [ ] Remote-reader caching stays at `RemoteFileSystem::open_file_impl()`, not reimplemented per backend?
