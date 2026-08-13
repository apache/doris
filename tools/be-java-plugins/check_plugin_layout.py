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
"""Check a deployed be/lib/java tree against the rules the plugin isolation depends on.

Run it on a built output tree:

    tools/be-java-plugins/check_plugin_layout.py [output/be/lib/java]

Four checks, each of which has caught a real regression during the plugin migration:

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
                 "<scope>provided</scope> so the SPI comes from lib/java/spi instead."
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
    root = argv[1] if len(argv) > 1 else os.path.join(
        os.environ.get("DORIS_HOME", "."), "output", "be", "lib", "java")
    spi_dir, plugins_dir = os.path.join(root, "spi"), os.path.join(root, "plugins")
    if not os.path.isdir(spi_dir) or not os.path.isdir(plugins_dir):
        sys.stderr.write("%s does not look like a deployed be/lib/java tree "
                         "(expected spi/ and plugins/ under it)\n" % root)
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
    plugins = sorted(d for d in os.listdir(plugins_dir)
                     if os.path.isdir(os.path.join(plugins_dir, d)))
    if not plugins:
        sys.stderr.write("no plugin directories under %s\n" % plugins_dir)
        return 2
    for plugin in plugins:
        pdir = os.path.join(plugins_dir, plugin)
        jars = sorted(os.path.join(pdir, f) for f in os.listdir(pdir) if f.endswith(".jar"))
        if not jars:
            fail("plugin-ships-no-spi", "plugin directory '%s' holds no jar" % plugin)
            continue
        print("checking %-18s %3d jars" % (plugin, len(jars)))
        check_plugin_ships_no_spi(plugin, jars, fail)
        check_duplicate_classes(plugin, jars, fail)
        check_closure_self_contained(plugin, jars, spi_jar, fail)

    if not failures:
        print("\nOK: %d plugins pass all four checks. This does NOT prove the closures are "
              "complete - see the note at the top of this file." % len(plugins))
        return 0
    print("\n%d problem(s):" % len(failures))
    for check, message in failures:
        print("\n[%s] %s" % (check, message))
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
