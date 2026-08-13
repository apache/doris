# Delivery options: in-tree dependency vs external plugin jar

The `openfga-doris` controller can be shipped two ways. They share all of the code in the
`org.apache.doris.catalog.authorizer.openfga` package; they differ only in where the OpenFGA SDK
dependency lives and how the factory is registered. The choice is a maintainer decision.

## Option A: in-tree (this branch)

The OpenFGA SDK is added to `fe/fe-core/pom.xml` and the factory is registered in
`fe/fe-core/src/main/resources/META-INF/services/org.apache.doris.mysql.privilege.AccessControllerFactory`.
The controller is then always on the classpath and is discovered by the normal ServiceLoader scan.

- Pros: single build, tested in CI with the rest of FE, no separate artifact to distribute.
- Cons: adds `dev.openfga:openfga-sdk` (and its transitive dependencies, including a JSON binding)
  to the FE dependency set. For an ASF release that means a license review, and likely shading or
  dependency-management entries to avoid version conflicts. This is the main merge-risk lever, so it
  should be agreed with the maintainers before it lands.

## Option B: external plugin jar

Doris already loads access controllers from an external plugin directory in addition to the
classpath ServiceLoader (see `AccessControllerManager.loadAccessControllerPlugins`). The same
package can be built as a standalone jar that bundles the OpenFGA SDK and a `META-INF/services`
entry, then dropped into the plugin directory. No change to the FE pom or the in-tree SPI file.

- Pros: zero new dependency in FE core, so no ASF license or shading review on the core release. The
  jar can iterate on its own cadence. It is a lower-friction first cut and still a complete,
  demonstrable artifact.
- Cons: the operator installs and version-manages the jar; it is not exercised by FE CI unless a
  regression job loads it; classloading and dependency isolation are the plugin author's
  responsibility.

## Recommendation

Start with Option B (external jar) to avoid the SDK-in-core merge risk while the design is being
agreed, then move to Option A if the maintainers want it in-tree. The code does not change between
the two; only the pom entry and the SPI registration move in or out. This branch includes the
in-tree wiring (Option A) so the full integration is reviewable in one place; both the pom entry and
the added SPI line can be reverted to produce the Option B shape.
