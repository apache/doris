# Lance JNI packaging for Linux x86_64

`bash build.sh --fe` automatically replaces the Linux x86_64 JNI entry in
`lance-core-11.0.0.jar` with the glibc 2.17 rebuild. No local Lance checkout,
Rust toolchain, or `LANCE_JNI_SO` variable is required on the Doris build host.

The artifact is pinned in `docker/thirdparties/lance-jni-helpers.sh` and published
in [apache/doris-thirdparty](https://github.com/apache/doris-thirdparty/releases/tag/lance-jni-11.0.0-glibc2.17-r1).
Both the compressed archive and the extracted library have fixed SHA256 values.

The archive is cached under:

```text
thirdparty/installed/lance-jni/liblance_jni-11.0.0-linux-x86_64-glibc2.17-r1.so.gz
```

With a custom `DORIS_THIRDPARTY`, the cache is under that directory's
`installed/lance-jni/`. A valid cache is reused without network access. A corrupt
cache is downloaded again. The existing `REPOSITORY_URL` mirror is tried first
when configured, followed by the pinned GitHub Release URL. Downloads use curl;
packaging also needs gzip, zip, unzip, and sha256sum.

To update an existing FE output without compiling Doris:

```bash
bash post-build.sh --fe
# Or use a custom output directory:
bash post-build.sh --fe --output /path/to/output
```

Only the JAR entry `nativelib/linux-x86-64/liblance_jni.so` is changed. The Maven
cache, Java classes, and other native entries are preserved. Download, checksum,
or JAR version failures stop packaging and leave the original output JAR intact.
ARM64, macOS, and BE-only packaging do not download or replace this library.

The rebuild retains Lance's Haswell CPU baseline (AVX2/FMA/F16C). Its GLIBC symbol
requirements were checked statically; a full CentOS 7 FE runtime test is still
required. An already running FE must restart to load a newly packaged JNI library.

When upgrading Lance, update the release URL, filename, version, and both hashes
in the helper together with `lance.version` in `fe/pom.xml`. Offline packaging
tests are available via `python3 thirdparty/test/lance-jni-packaging-test.py`.
