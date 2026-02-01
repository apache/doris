# fe-extension-spi

This module defines the stable SPI contracts for the Doris FE plugin system.
It is intended to be reused by authentication, authorization, and protocol
extensions.

Scope:
- Plugin metadata contract: PluginDescriptor
- Plugin lifecycle contract: Plugin
- Factory contract: PluginFactory
- Runtime context: PluginContext
- Common error type: PluginException

Service discovery:
- Factories are discovered via ServiceLoader.
- Each plugin jar must provide:
  META-INF/services/org.apache.doris.extension.spi.PluginFactory

Versioning:
- PluginDescriptor.spiVersion is reserved for compatibility checks.
- SPI evolution should be backward compatible within the same major version.

Non-goals:
- No IO or loading logic lives here. See fe-extension-loader.
