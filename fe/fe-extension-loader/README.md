# fe-extension-loader

This module provides the plugin loading infrastructure for Doris FE.
It is generic and can be reused by authentication, authorization, and protocol
plugins.

Plugin package layout (recommended):
- plugin-name/
  - plugin.properties
  - plugin.jar
  - lib/

plugin.properties fields:
- name: plugin id
- version: plugin version
- spiVersion: SPI version (integer)
- factoryClass: fully qualified PluginFactory implementation

Classloading strategy:
- Child-first for plugin code and dependencies.
- Parent-first allowlist for core APIs:
  java.*, javax.*, sun.*, com.sun.*, org.slf4j.*, org.apache.logging.*,
  org.apache.doris.extension.spi.*

Loading flow (logical steps):
1) Read plugin.properties (IO layer; not implemented yet).
2) Parse properties into PluginDescriptor.
3) Build ChildFirstClassLoader for plugin.jar + lib/.
4) Use ServiceLoader to find PluginFactory.
5) Create and initialize Plugin instance.

Reloading:
- Close the plugin ClassLoader and discard instances before reloading.
