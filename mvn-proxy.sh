#!/usr/bin/env bash
# Sandbox-only wrapper: route maven through the sandbox egress proxy and
# force the JDK-17 toolchain (the mise `mvn` native shim ignores JAVA_HOME).
export JAVA_HOME="${JAVA_HOME:-/root/.local/share/mise/installs/java/17.0.2}"
exec /root/.local/share/mise/installs/maven/3.9.10/apache-maven-3.9.10/bin/mvn -s /workspace/.mvn-settings.xml "$@"
