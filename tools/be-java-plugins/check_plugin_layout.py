#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Check a deployed BE plugin tree against the rules the plugin isolation depends on.

Run it on a built output tree:

    tools/be-java-plugins/check_plugin_layout.py [output/be/lib/jni/spi] [output/be/plugins/jni]

Five checks, four of which have caught a real regression during the plugin migration:

  spi-jar-purity        doris-jni-spi.jar carries only the SPI packages. Everything in that jar
                        is shared by BE and by every plugin at once, so a class that slips in
                        can never be upgraded by one plugin alone.
  plugin-ships-no-spi   No jar inside a plugin directory carries an SPI class. A plugin that
                        brings its own copy gets a second, incompatible identity for the same
                        type; the loader rejects it, but only when someone runs a query.
  duplicate-classes     Within one plugin directory, a class name that resolves to different
                        bytes in different jars. One directory is one flat classloader, so the
                        winner is jar-name order - not a decision anyone made.
  closure-self-contained
                        Every class referenced from a plugin's ROOT jars resolves inside that
                        plugin directory (plus the SPI jar and the JDK). The roots are Doris's own
                        jars AND every jar holding a class Doris hands to hadoop as a string -
                        the fs.*.impl and credentials-provider values in fe/fe-filesystem/**, see
                        _NAMED_BY_DORIS. This is the check that fires when a dependency is
                        switched to `provided` and its jar stops being deployed while the code
                        still calls into it, and - because of that second root set - when a
                        bundled filesystem is missing a library of its OWN.
  api-version-stamp     The jar declaring the DorisPlugin service carries the
                        Doris-Jni-Plugin-Api-Version this build serves. That attribute is the only
                        thing PluginRuntime's version gate reads, and until this check existed
                        nothing verified it reached the artifact.

WHAT THIS DOES NOT PROVE. The last check is static, so it says nothing about anything reached by
a ServiceLoader or by a reflective lookup Doris does not make itself - JDBC drivers, logging
backends, jackson modules. A jar can disappear from a plugin directory, pass every check here, and
still fail at runtime. The only thing that proves a closure complete is loading the plugin out of
the deployed directory for real. Treat a green run as "no STATICALLY visible hole", nothing more.

BUNDLED THIRD-PARTY FILESYSTEMS used to be the sharpest edge of that and have drawn blood twice. A
plugin packages hadoop-aws or hadoop-huaweicloud for one URI scheme; hadoop reaches it by class
NAME out of a Configuration, so no Doris class references it and a walk rooted at Doris's own jars
never entered it. Its own dependencies were then invisible too - hadoop-huaweicloud calls
commons-lang 2.x from seven classes and hadoop-aws's AssumedRoleCredentialProvider calls
software.amazon.awssdk.services.sts from twenty-three, and neither jar carries a copy. Both used to
come from the shared preload classpath and now have to be declared by each plugin that bundles
them; both shipped broken before this was checked.

_NAMED_BY_DORIS closes that particular hole by making those jars roots. Note what it is NOT: it
does not follow a Class.forName and it is not reflection analysis. It is a hand-maintained list of
the class names Doris itself writes into a Configuration, and every reference it then finds is an
ordinary constant-pool entry. A filesystem Doris reaches some other way is still invisible, and so
is anything those roots reach reflectively - the per-plugin
resolvesEveryFilesystemSchemeAScanCanArriveOn tests do not close that either, because
getFileSystemClass returns a Class without linking it.

The shared filesystem directory (plugins/jni_fs, appended to every plugin classloader for
jindofs/juicefs) is outside this check entirely: it is not a plugin directory, so nothing here
looks at it - not the duplicate scan, and not the closure walk, which will happily report a class
as missing that a BE resolves out of there at runtime.

Exit status: 0 if every check passes, 1 if any fails, 2 if the tree or the tools are unusable.
"""
import collections
import functools
import os
import subprocess
import sys
import zipfile

SPI_PREFIX = "org/apache/doris/jni/spi/"
DORIS_PREFIX = "org/apache/doris/"

_OBS = ("Two builds of the Huawei OBS SDK: the standalone esdk-obs-java-optimised and the copy "
        "shaded into the hadoop-huaweicloud fat jar. Both are present on BE's system classpath "
        "today with the same overlap, and PluginRuntime sorts jar URLs, so the copy that wins "
        "here is the copy that wins today.")

# Classes allowed to appear with DIFFERING bytes in two jars of the same plugin directory.
# Adding an entry means claiming the arbitrary winner is harmless - write down why.
# Match kinds: "basename" compares the last path segment, "prefix" compares the whole path.
DUPLICATE_ALLOWLIST = {
    "*": [
        ("basename", "module-info.class",
         "A plugin directory is a classpath, not a module path, so module descriptors are never "
         "read. Multi-release jars additionally carry one per version directory."),
        ("basename", "package-info.class",
         "Holds package annotations and no code. The copies that collide here are hadoop's, "
         "which carry only its InterfaceAudience/InterfaceStability documentation annotations, "
         "so which one wins changes nothing that executes."),
    ],
    # No JindoFS entries any more. This build no longer copies the JindoFS jars into the iceberg
    # and paimon directories - plugins/jni_fs is appended to every plugin classloader instead - so
    # the ten class paths those copies used to collide on (org/apache/hadoop/fs/StreamCapabilities,
    # PositionedReadable, four javax/annotation Checker types and their siblings) are back under
    # this check. A real collision on any of them must fail the build rather than be waved through
    # by an exemption whose reason no longer exists.
    "iceberg": [("prefix", "com/obs/", _OBS), ("prefix", "com/oef/", _OBS)],
    "paimon": [("prefix", "com/obs/", _OBS), ("prefix", "com/oef/", _OBS)],
    "hudi": [("prefix", "com/obs/", _OBS), ("prefix", "com/oef/", _OBS)],
}

# Classes that Doris's own code in a plugin references but that the plugin deliberately does not
# ship. Every entry is a claim that the referencing code is never executed in BE - the class is
# in the jar because it came with a shared Doris artifact, not because a plugin path reaches it.
# Prefix match on the unresolved class name.
_TRIMMED = ("Reached only from {} inside a shared Doris jar that the plugin needs for other "
            "reasons. {} was excluded from this closure on purpose; the referencing code is not "
            "on any BE path, so the class is never loaded and the missing library never resolves.")
_FASTUTIL = _TRIMMED.format(
    "fe-foundation's ConcurrentLong2LongHashMap / ConcurrentLong2ObjectHashMap", "fastutil")
# Holes INSIDE the bundled cloud filesystems, surfaced by the widened root set (_NAMED_BY_DORIS).
# These are not Doris's code and not Doris's choices; each entry says why the reference is never
# linked on a BE path, and every one of them was checked with javap against the deployed jar
# rather than assumed. Adding hadoop-aws or hadoop-huaweicloud to a plugin brings the whole list.
_S3A_CSE = ("hadoop-aws's EncryptionS3ClientFactory, the S3 client-side-encryption path "
            "(fs.s3a.encryption.algorithm=CSE-KMS). It needs the Amazon S3 Encryption Client "
            "(software.amazon.encryption.s3), which is not a transitive dependency of anything "
            "here and was not on the shared preload classpath this plugin layout replaced either "
            "- CSE-KMS therefore did not work before this change and does not now. Shipping the "
            "kms module alone would not fix it. Doris never writes that property.")
_SDK_BUNDLE = ("the AWS SDK's own shaded HTTP client, which ships only in its `bundle` uber-jar; "
               "the plugins deploy the modular `apache-client` instead. The two reference sites "
               "are STSClientFactory.getSTSEndpoint (private, called only when "
               "fs.s3a.assumed.role.sts.endpoint is set - Doris never writes it) and "
               "ConfigureShadedAWSSocketFactory (reached only for fs.s3a.ssl.channel.mode values "
               "naming the shaded OpenSSL path, which Doris never writes either).")
_SDK_V1 = ("hadoop-aws's V1ToV2AwsCredentialProviderAdapter, the shim for credential providers "
           "written against AWS SDK v1. It is instantiated only for a provider CLASS NAME under "
           "com.amazonaws.*, and every provider Doris names is either hadoop's own or an SDK v2 "
           "one (see _NAMED_BY_DORIS). No plugin ships SDK v1 at all.")
_LOG4J1 = ("hadoop-aws's Log4JController, an optional log4j 1.x bridge that LogControllerFactory "
           "loads reflectively and skips when it is absent. Doris runs log4j2.")
_OKHTTP_PLATFORM = ("the OBS SDK's shaded okhttp, whose Platform.findPlatform() probes for an "
                    "Android or Conscrypt runtime and catches ClassNotFoundException. Neither is "
                    "present in a BE, which is what the probe is for.")
_OBS_BASE64 = ("the OBS SDK's shaded XML builder, which prefers net.iharder.Base64 when it is on "
               "the classpath and falls back to its own encoder when it is not.")
_GUAVA_CHECKED_FUTURE = ("Futures.immediateFailedCheckedFuture, removed in Guava 26; "
                         "hadoop-huaweicloud 3.1.1-hw-46 was built against an older one. Reached "
                         "only from SemaphoredDelegatingExecutor.submit()'s InterruptedException "
                         "branch. Pre-existing and byte-identical to what BE's system classpath "
                         "carried before this layout - no plugin can fix it without shipping a "
                         "Guava that the rest of the tree has moved past.")
_S3A_COMMITTER = ("hadoop-aws's S3A output committers, which extend hadoop-mapreduce-client-core "
                  "types. BE reads table formats; it never runs a MapReduce job, and nothing "
                  "here resolves a committer. NOT in _BUNDLED_S3A on purpose: the prefix is far "
                  "wider than the committers and covers "
                  "org.apache.hadoop.mapreduce.lib.input.FileInputFormat, which paimon and hudi "
                  "DO reach - see the per-plugin note in CLOSURE_ALLOWLIST.")
_BUNDLED_S3A = [
    ("software.amazon.encryption.s3.", _S3A_CSE),
    ("software.amazon.awssdk.services.kms.", _S3A_CSE),
    ("software.amazon.awssdk.thirdparty.", _SDK_BUNDLE),
    ("com.amazonaws.", _SDK_V1),
    ("org.apache.log4j.", _LOG4J1),
]
_BUNDLED_OBS = [
    ("android.", _OKHTTP_PLATFORM),
    ("org.conscrypt.", _OKHTTP_PLATFORM),
    ("net.iharder.", _OBS_BASE64),
    ("com.google.common.util.concurrent.CheckedFuture", _GUAVA_CHECKED_FUTURE),
]
CLOSURE_ALLOWLIST = {
    # The mapreduce exemption is iceberg's ALONE. iceberg-metadata-scanner is the only plugin that
    # bundles hadoop-aws WITHOUT hadoop-mapreduce-client-core, so it is the only one where the S3A
    # committers' mapreduce references are genuinely unresolvable. paimon-scanner and
    # hadoop-hudi-scanner declare that artifact on purpose (commit d50394e645d: every parquet read
    # from a paimon catalog failed with NoClassDefFoundError on
    # org.apache.hadoop.mapreduce.lib.input.FileInputFormat, which this prefix would wave through),
    # so on those two the prefix stays guarded and dropping the jar again fails the build.
    "iceberg": [("it.unimi.dsi.fastutil.", _FASTUTIL),
                ("org.apache.hadoop.mapreduce.", _S3A_COMMITTER)] + _BUNDLED_S3A + _BUNDLED_OBS,
    "paimon": [("it.unimi.dsi.fastutil.", _FASTUTIL)] + _BUNDLED_S3A + _BUNDLED_OBS,
    "hudi": [("it.unimi.dsi.fastutil.", _FASTUTIL)] + _BUNDLED_S3A + _BUNDLED_OBS,
    "java-udf": [
        ("it.unimi.dsi.fastutil.", _FASTUTIL),
        ("org.roaringbitmap.", _TRIMMED.format("fe-common's org.apache.doris.common.io codecs",
                                               "RoaringBitmap")),
        ("com.fasterxml.jackson.", _TRIMMED.format("fe-common's org.apache.doris.common.util",
                                                   "jackson")),
        ("com.google.protobuf.", _TRIMMED.format("fe-common's org.apache.doris.persist.gson",
                                                 "protobuf")),
        ("io.netty.", _TRIMMED.format("fe-common's org.apache.doris.common", "netty")),
        ("org.apache.commons.io.", _TRIMMED.format("fe-common's org.apache.doris.common",
                                                   "commons-io")),
        ("org.apache.commons.codec.", _TRIMMED.format("fe-common's org.apache.doris.common.io",
                                                      "commons-codec")),
        ("org.apache.logging.log4j.core.", _TRIMMED.format(
            "fe-common's log4j plugin classes", "log4j-core")),
    ],
}


def _allowed(rules, path):
    for kind, pattern, _reason in rules:
        if kind == "basename" and path.rsplit("/", 1)[-1] == pattern:
            return True
        if kind == "prefix" and path.startswith(pattern):
            return True
    return False


def _class_entries(jar):
    with zipfile.ZipFile(jar) as zf:
        return [(i.filename, i.CRC) for i in zf.infolist() if i.filename.endswith(".class")]


# Memoized because the same jar is read by four of the five checks - the duplicate scan, the
# sole-provider scan, the api-version scan and the root selection below - and a lake-format
# plugin directory holds a hundred of them, one of which is a 180 MB fat jar. A tuple rather than
# the list zipfile hands back, so a caller cannot mutate what the next one reads.
@functools.lru_cache(maxsize=None)
def _names(jar):
    with zipfile.ZipFile(jar) as zf:
        return tuple(zf.namelist())


def check_spi_jar_purity(spi_dir, fail):
    jar = os.path.join(spi_dir, "doris-jni-spi.jar")
    # Both jars, because the shared layer is both: without doris-jni-bootstrap.jar there is no
    # loader at all and every Java feature fails at runtime with a FindClass error naming none of
    # this. Only the SPI jar's contents are checked below - the loader may carry whatever it needs.
    loader = os.path.join(spi_dir, "doris-jni-bootstrap.jar")
    if not os.path.isfile(loader):
        fail("spi-jar-purity",
             "%s is missing; without the loader no plugin can be loaded at all" % loader)
    if not os.path.isfile(jar):
        fail("spi-jar-purity", "%s is missing; the shared layer was not deployed" % jar)
        return
    stray = [n for n in _names(jar)
             if not n.endswith("/")
             and not n.startswith(SPI_PREFIX)
             and not n.startswith("META-INF/")]
    for n in sorted(stray):
        fail("spi-jar-purity",
             "doris-jni-spi.jar contains %s. Only %s and META-INF/ may be in this jar - it is "
             "loaded once and shared by BE and every plugin." % (n, SPI_PREFIX))


def check_plugin_ships_no_spi(plugin, jars, fail):
    for jar in jars:
        carried = [n for n in _names(jar) if n.startswith(SPI_PREFIX) and n.endswith(".class")]
        if carried:
            fail("plugin-ships-no-spi",
                 "plugin '%s': %s carries %d SPI class(es), e.g. %s. Declare jni-spi as "
                 "<scope>provided</scope> so the SPI comes from lib/jni/spi instead."
                 % (plugin, os.path.basename(jar), len(carried), carried[0]))


def check_duplicate_classes(plugin, jars, fail):
    rules = DUPLICATE_ALLOWLIST.get("*", []) + DUPLICATE_ALLOWLIST.get(plugin, [])
    by_name = collections.defaultdict(lambda: collections.defaultdict(list))
    for jar in jars:
        for name, crc in _class_entries(jar):
            by_name[name][crc].append(os.path.basename(jar))
    for name in sorted(by_name):
        by_crc = by_name[name]
        if len(by_crc) < 2 or _allowed(rules, name):
            continue
        where = "; ".join("%08x in %s" % (crc, ", ".join(sorted(set(js))))
                          for crc, js in sorted(by_crc.items()))
        fail("duplicate-classes",
             "plugin '%s': %s resolves to different bytes depending on jar order (%s). Keep one "
             "copy, or add it to DUPLICATE_ALLOWLIST with the reason the winner does not matter."
             % (plugin, name, where))


SERVICE_ENTRY = "META-INF/services/org.apache.doris.jni.spi.DorisPlugin"
API_VERSION_ATTRIBUTE = "Doris-Jni-Plugin-Api-Version"
API_VERSION_RESOURCE = "META-INF/doris/jni-plugin-api-version.properties"


def _manifest_attribute(jar, attribute):
    with zipfile.ZipFile(jar) as zf:
        try:
            manifest = zf.read("META-INF/MANIFEST.MF").decode("utf-8", "replace")
        except KeyError:
            return None
    # Manifest continuation lines start with a single space; unfold before matching.
    manifest = manifest.replace("\r\n", "\n").replace("\r", "\n").replace("\n ", "")
    for line in manifest.split("\n"):
        name, sep, value = line.partition(":")
        if sep and name.strip() == attribute:
            return value.strip()
    return None


def served_api_version(spi_jar):
    """The major.minor this build serves, out of the resource maven filtering writes."""
    with zipfile.ZipFile(spi_jar) as zf:
        try:
            text = zf.read(API_VERSION_RESOURCE).decode("utf-8", "replace")
        except KeyError:
            return None
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == "api.version":
            return value.strip()
    return None


def check_api_version_stamp(plugin, jars, served, fail):
    """
    The manifest attribute PluginRuntime.checkApiVersion compares against, checked on the artifact
    rather than in the pom that is supposed to produce it. A plugin jar built without the parent's
    <archive> configuration - a module that overrides maven-jar-plugin, say - carries no stamp or a
    stale one, and nothing else notices: the loader is the first to look, at which point the failure
    is a deployment-time "built against plugin API 2.0 but this BE serves 3.0" on a jar that was
    never built against 2.0 at all.

    Only the jar declaring the service is FAILED on, because that is the only one the loader reads.

    Every other stamped jar in the directory is reported as a warning instead. Nothing at runtime
    reads those stamps, so a mismatch there is not a defect in the deployed tree - but it is a
    reliable sign of a jar that was not rebuilt, which nothing else in this script can see (this
    check reads every jar's entry list, which _names memoizes; the closure check does not care
    when a class was compiled).
    That is not hypothetical: a tree where every jar was stamped 3.0 except one left over at 2.0
    passed all five checks, and the leftover was a stale build artifact, not a deliberate one.
    """
    if served is None:
        fail("api-version-stamp",
             "the SPI jar carries no %s; the version gate has nothing to compare against"
             % API_VERSION_RESOURCE)
        return
    _warn_stale_stamps(plugin, jars, served)
    for jar in jars:
        if SERVICE_ENTRY not in _names(jar):
            continue
        declared = _manifest_attribute(jar, API_VERSION_ATTRIBUTE)
        if declared is None:
            fail("api-version-stamp",
                 "plugin '%s': %s declares the DorisPlugin service but its manifest carries no %s. "
                 "It comes from <jni.plugin.api.version> in fe/be-java-extensions/pom.xml; a module "
                 "that redefines maven-jar-plugin's <archive> loses it."
                 % (plugin, os.path.basename(jar), API_VERSION_ATTRIBUTE))
        elif declared != served:
            fail("api-version-stamp",
                 "plugin '%s': %s is stamped %s=%s but this build serves %s. Rebuild the module; a "
                 "mismatch here is what PluginRuntime.checkApiVersion rejects at deployment."
                 % (plugin, os.path.basename(jar), API_VERSION_ATTRIBUTE, declared, served))
        return
    # No provider jar at all is the sole-provider check's verdict to give, not this one's.


def _warn_stale_stamps(plugin, jars, served):
    """Stamped jars that do NOT declare the service and disagree with the served version.

    A warning, never a failure: the loader reads the service jar's stamp and nothing else, so
    asserting on these would be asserting on something no runtime consults - and a plugin is
    free to bundle a third-party jar that carries its own unrelated build stamps.
    """
    for jar in jars:
        if SERVICE_ENTRY in _names(jar):
            continue
        declared = _manifest_attribute(jar, API_VERSION_ATTRIBUTE)
        if declared is not None and declared != served:
            print("  WARN plugin '%s': %s is stamped %s=%s while this build serves %s. Nothing "
                  "reads that stamp, so this is not a failure - but it usually means the jar was "
                  "not rebuilt." % (plugin, os.path.basename(jar), API_VERSION_ATTRIBUTE,
                                    declared, served))


def _doris_owned(jars):
    owned = []
    for jar in jars:
        if any(n.startswith(DORIS_PREFIX) and n.endswith(".class") for n in _names(jar)):
            owned.append(jar)
    return owned


# Implementation classes Doris hands to hadoop AS A STRING, so that no Doris class references
# them and the walk from Doris's own jars never enters the jar they live in. Every one of these
# is written into a Configuration by fe/fe-filesystem/**; the list is short because the write
# points are - fs.*.impl and the two credentials-provider keys, nothing else.
#
# Why this matters more than it looks: hadoop-aws's AssumedRoleCredentialProvider resolves
# software.amazon.awssdk.services.sts.* from its constant pool, and hadoop-huaweicloud's
# OBSFileSystem resolves org.apache.commons.lang.*. Both are ordinary static references that jdeps
# reports the moment the jar holding them is a root - and both were real, shipped holes that every
# other check here passed over. Treating the jar as a root is not the same as trusting reflection
# analysis: nothing here follows a Class.forName, it just starts the walk somewhere Doris pointed.
#
# Entries are class file paths. A name absent from a plugin costs nothing: that plugin simply
# bundles no such filesystem, and the root set is what it was.
_NAMED_BY_DORIS = (
    # fs.s3.impl / fs.s3a.impl / fs.cos.impl / fs.cosn.impl / fs.gs.impl, and the fallback
    # fs.obs.impl - one jar, hadoop-aws, and the S3A credential providers live in it too.
    "org/apache/hadoop/fs/s3a/S3AFileSystem.class",
    "org/apache/hadoop/fs/s3a/SimpleAWSCredentialsProvider.class",
    "org/apache/hadoop/fs/s3a/auth/AssumedRoleCredentialProvider.class",
    # fs.obs.impl and fs.AbstractFileSystem.obs.impl - hadoop-huaweicloud.
    "org/apache/hadoop/fs/obs/OBSFileSystem.class",
    "org/apache/hadoop/fs/obs/OBS.class",
    # fs.s3a.aws.credentials.provider and fs.s3a.assumed.role.credentials.provider, when the
    # catalog names a provider type rather than a key pair - the AWS SDK's own auth module.
    "software/amazon/awssdk/auth/credentials/WebIdentityTokenFileCredentialsProvider.class",
    "software/amazon/awssdk/auth/credentials/InstanceProfileCredentialsProvider.class",
    "software/amazon/awssdk/auth/credentials/ContainerCredentialsProvider.class",
    # fs.oss.impl / fs.AbstractFileSystem.oss.impl. Deployed to plugins/jni_fs rather than into a
    # plugin directory, so normally absent here; listed so that a plugin which does bundle
    # JindoFS gets the same treatment as the rest.
    "com/aliyun/jindodata/oss/JindoOssFileSystem.class",
)


def _named_by_doris(jars):
    """Jars holding a class Doris names by string. See _NAMED_BY_DORIS."""
    wanted = set(_NAMED_BY_DORIS)
    return [jar for jar in jars if wanted.intersection(_names(jar))]


def check_closure_self_contained(plugin, jars, spi_jar, fail):
    owned = _doris_owned(jars)
    if not owned:
        fail("closure-self-contained",
             "plugin '%s' has no jar containing %s classes; it cannot implement the SPI."
             % (plugin, DORIS_PREFIX))
        return
    roots = owned + [jar for jar in _named_by_doris(jars) if jar not in owned]
    classpath = os.pathsep.join(jars + [spi_jar])
    proc = subprocess.run(["jdeps", "--multi-release", "17", "-verbose:class", "-cp", classpath]
                          + roots,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True)
    if proc.returncode != 0:
        # Never treat this as a pass. An earlier version of this script analysed every jar in the
        # directory; jdeps then built a module graph, died on a module-info requiring something
        # absent, printed nothing, and the run looked clean.
        fail("closure-self-contained",
             "plugin '%s': jdeps exited %d, so nothing was checked.\n%s"
             % (plugin, proc.returncode, proc.stderr.strip()[:600]))
        return
    rules = CLOSURE_ALLOWLIST.get(plugin, [])
    missing = collections.defaultdict(set)
    for line in proc.stdout.splitlines():
        if "not found" not in line:
            continue
        parts = line.split("->")
        if len(parts) != 2:
            continue
        src, target = parts[0].strip(), parts[1].strip().split()[0]
        if "." not in src or "." not in target:
            continue
        if any(target.startswith(prefix) for prefix, _reason in rules):
            continue
        missing[target].add(src)
    for target in sorted(missing):
        fail("closure-self-contained",
             "plugin '%s': %s is referenced by %s but is in no jar of this plugin directory. "
             "Either ship the jar it lives in, or add the package to CLOSURE_ALLOWLIST with the "
             "reason that code never runs."
             % (plugin, target, sorted(missing[target])[0]))


def main(argv):
    if len(argv) > 1 and argv[1] in ("-h", "--help"):
        print(__doc__)
        return 0
    home = os.environ.get("DORIS_HOME", ".")
    spi_dir = argv[1] if len(argv) > 1 else os.path.join(
        home, "output", "be", "lib", "jni", "spi")
    plugins_dir = argv[2] if len(argv) > 2 else os.path.join(
        home, "output", "be", "plugins", "jni")
    if not os.path.isdir(spi_dir):
        sys.stderr.write("%s is not a deployed spi directory\n" % spi_dir)
        return 2
    if not os.path.isdir(plugins_dir):
        sys.stderr.write("%s is not a deployed plugin family root\n" % plugins_dir)
        return 2
    try:
        subprocess.run(["jdeps", "--version"], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    except OSError:
        sys.stderr.write("jdeps not on PATH; it ships with the JDK this build already needs\n")
        return 2

    failures = []

    def fail(check, message):
        failures.append((check, message))

    check_spi_jar_purity(spi_dir, fail)
    spi_jar = os.path.join(spi_dir, "doris-jni-spi.jar")
    served = served_api_version(spi_jar) if os.path.isfile(spi_jar) else None
    plugins = sorted(d for d in os.listdir(plugins_dir)
                     if os.path.isdir(os.path.join(plugins_dir, d)))
    if not plugins:
        sys.stderr.write("no plugin directories under %s\n" % plugins_dir)
        # Not "could not run" when the shared layer has already been found broken: 2 is mapped to a
        # WARN by build.sh, which would report a real SPI failure as a check that did not happen.
        # When there ARE failures this falls through to the reporting tail instead of returning
        # here, because this is the only place they get printed and build.sh says "see above".
        if not failures:
            return 2
        plugins = []
    for plugin in plugins:
        pdir = os.path.join(plugins_dir, plugin)
        jars = sorted(os.path.join(pdir, f) for f in os.listdir(pdir) if f.endswith(".jar"))
        if not jars:
            fail("plugin-directory-nonempty", "plugin directory '%s' holds no jar" % plugin)
            continue
        print("checking %-18s %3d jars" % (plugin, len(jars)))
        check_plugin_ships_no_spi(plugin, jars, fail)
        check_duplicate_classes(plugin, jars, fail)
        check_closure_self_contained(plugin, jars, spi_jar, fail)
        check_api_version_stamp(plugin, jars, served, fail)

    if not failures:
        print("\nOK: %d plugins pass all five checks. This does NOT prove the closures are "
              "complete - see the note at the top of this file." % len(plugins))
        return 0
    print("\n%d problem(s):" % len(failures))
    for check, message in failures:
        print("\n[%s] %s" % (check, message))
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
