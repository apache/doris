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
                        Every class that Doris's OWN code in a plugin references resolves inside
                        that plugin directory (plus the SPI jar and the JDK). This is the check
                        that fires when a dependency is switched to `provided` and its jar stops
                        being deployed while the code still calls into it.
  api-version-stamp     The jar declaring the DorisPlugin service carries the
                        Doris-Jni-Plugin-Api-Version this build serves. That attribute is the only
                        thing PluginRuntime's version gate reads, and until this check existed
                        nothing verified it reached the artifact.

WHAT THIS DOES NOT PROVE. The last check is static and starts from Doris's own classes only, so
it says nothing about anything reached by ServiceLoader or reflection - filesystem providers,
JDBC drivers, logging backends, jackson modules. A jar can disappear from a plugin directory,
pass every check here, and still fail at runtime. The only thing that proves a closure complete
is loading the plugin out of the deployed directory for real. Treat a green run as "no
STATICALLY visible hole", nothing more.

Exit status: 0 if every check passes, 1 if any fails, 2 if the tree or the tools are unusable.
"""
import collections
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

_JINDO = ("build.sh copies the JindoFS jars into the iceberg and paimon plugin directories after "
          "post-build.sh packages them (DISABLE_BUILD_JINDOFS=OFF), because a plugin classloader "
          "cannot reach the system classpath those jars otherwise live on and paimon-jindo / "
          "fs.oss.impl name com.aliyun.jindodata.* as the oss:// implementation. jindo-sdk carries "
          "a handful of hadoop and jsr305 classes of its own, which is what collides. ")

# WHY THE WINNER DOES NOT MATTER, entry by entry - derived by comparing the two copies with javap,
# not by assuming. PluginRuntime.jarsIn() sorts by file name, so the winner is the alphabetically
# first jar: hadoop-common-3.4.2.jar beats jindo-sdk-6.10.4.jar ('h' < 'j'), and jindo-sdk beats
# jsr305-3.0.2.jar ('j','i' < 'j','s').
#
# CAVEAT: that ordering is an accident of two file names, and either version can be bumped without
# anyone here noticing - jindofs to a name sorting before hadoop-common would silently flip every
# fs/** collision. Re-derive these entries (javap both copies, compare) whenever either version
# moves, and name them one by one rather than allowing a prefix, so that a NEW collision the next
# version brings still fails the build instead of being waved through.
_JINDO_HADOOP_FS = [
    ("prefix", "org/apache/hadoop/fs/StreamCapabilities.class",
     _JINDO + "hadoop-common wins and its copy is a superset: it declares one constant more "
     "(VECTOREDIO_BUFFERS_SLICED) and is otherwise identical."),
    ("prefix", "org/apache/hadoop/fs/StreamCapabilities$StreamCapability.class",
     _JINDO + "hadoop-common wins; the two copies are semantically identical."),
    ("prefix", "org/apache/hadoop/fs/PositionedReadable.class",
     _JINDO + "hadoop-common wins, and it is the more capable copy: it implements readVectored "
     "where jindo-sdk's throws UnsupportedOperationException."),
    ("prefix", "org/apache/hadoop/fs/impl/AbstractFSBuilderImpl.class",
     _JINDO + "hadoop-common wins; the two copies are semantically identical."),
]

_JINDO_JSR305 = [
    ("prefix", name,
     _JINDO + "jindo-sdk wins over jsr305, and the two copies are semantically identical - these "
     "are annotation types and their trivial nested Checker classes.")
    for name in (
        "javax/annotation/MatchesPattern$Checker.class",
        "javax/annotation/Nonnegative$Checker.class",
        "javax/annotation/Nonnull$Checker.class",
        "javax/annotation/RegEx$Checker.class",
        "javax/annotation/Syntax.class",
        "javax/annotation/meta/When.class",
    )
]

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
    "iceberg": [("prefix", "com/obs/", _OBS), ("prefix", "com/oef/", _OBS)] + _JINDO_HADOOP_FS
              + _JINDO_JSR305,
    "paimon": [("prefix", "com/obs/", _OBS), ("prefix", "com/oef/", _OBS)] + _JINDO_HADOOP_FS,
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
CLOSURE_ALLOWLIST = {
    "iceberg": [("it.unimi.dsi.fastutil.", _FASTUTIL)],
    "paimon": [("it.unimi.dsi.fastutil.", _FASTUTIL)],
    "hudi": [("it.unimi.dsi.fastutil.", _FASTUTIL)],
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


def _names(jar):
    with zipfile.ZipFile(jar) as zf:
        return zf.namelist()


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

    Only the jar declaring the service is checked, because that is the only one the loader reads.
    """
    if served is None:
        fail("api-version-stamp",
             "the SPI jar carries no %s; the version gate has nothing to compare against"
             % API_VERSION_RESOURCE)
        return
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


def _doris_owned(jars):
    owned = []
    for jar in jars:
        if any(n.startswith(DORIS_PREFIX) and n.endswith(".class") for n in _names(jar)):
            owned.append(jar)
    return owned


def check_closure_self_contained(plugin, jars, spi_jar, fail):
    roots = _doris_owned(jars)
    if not roots:
        fail("closure-self-contained",
             "plugin '%s' has no jar containing %s classes; it cannot implement the SPI."
             % (plugin, DORIS_PREFIX))
        return
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
        return 1 if failures else 2
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
