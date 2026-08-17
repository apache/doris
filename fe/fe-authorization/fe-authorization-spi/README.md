# Doris FE Authorization SPI

This is the plugin author's quickstart: what to implement, how to package it and
how to install it. For the framework around it — the module map, how the engine
discovers and routes to a source, and a step-by-step walkthrough for a new
source — read `../README.md`; for build recipes and the obligations a change
here carries, `../AGENTS.md`.

## Overview

`fe-authorization-spi` defines the plugin contract for authorization in Doris FE: an *authorization source*
decides, for the resources it governs, what a user may do with them.

Plugin authors implement:
- `AuthorizationPlugin` — the decisions
- `AuthorizationPluginFactory` — how the engine builds one

Both are discovered via Java `ServiceLoader`. The decision vocabulary (`AuthorizedSubject`,
`AuthorizedResource`, `AccessAction`, `AccessRequirement`, …) lives in `fe-authorization-api`, which this
module depends on and which is part of the same frozen contract.

## What the engine promises

**One source answers, and its answer is the whole answer.** Which source is asked follows from the resource
alone: the plugin a catalog is bound to answers for everything inside that catalog, the plugin installed for
the instance answers for everything else. Nothing grants access before a plugin is asked, and no second
plugin is consulted after it. Exemptions that used to be the engine's — "an administrator may go anywhere" —
are each plugin's own to grant or refuse, with `AuthorizationContext` there to ask the questions such a
decision needs.

**Refusing is throwing.** A check that returns has allowed the access; a check that refuses throws
`AccessDeniedException`. There is no third outcome and no boolean for a caller to ignore.

**Every check method defaults to refusing.** A plugin that implements nothing but `name()` denies
everything — an omission costs you access control you did not think about, never a hole. The two
data-policy methods are the exception: their empty default means "this source defines no policy", which is
not the same as allowing anything.

## Minimal plugin

```java
public final class CustomAuthorizationPlugin implements AuthorizationPlugin {

    private final AuthorizationContext context;

    CustomAuthorizationPlugin(AuthorizationContext context) {
        this.context = context;
    }

    @Override
    public String name() {
        return "custom-authz";
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext ctx) throws AccessDeniedException {
        // Whoever owns instance scope may already have settled this; asking spares a second evaluation
        // of the same policies. Answers false when this plugin IS that authority.
        if (context.grantedByGlobalScopeAuthority(subject, requirement)) {
            return;
        }
        if (!(resource instanceof AuthorizedResource.Table)) {
            // A kind this source does not recognise is a refusal, never a guess.
            throw AccessDeniedException.of(subject, resource, requirement, name());
        }
        AuthorizedResource.Table table = (AuthorizedResource.Table) resource;
        if (!allowed(context.rolesOf(subject), table, requirement)) {
            throw AccessDeniedException.of(subject, resource, requirement, name());
        }
    }

    @Override
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext ctx) {
        // The predicate is SQL in Doris dialect; the engine parses, type-checks and plans it. The ident
        // names the policy it came from, and only shows up in diagnostics. Several filters on one table are
        // combined as each one's merge type says: RESTRICTIVE ones are ANDed, PERMISSIVE ones ORed.
        return Collections.singletonList(RowFilterSpec.restrictive("eu-only", "region = 'EU'"));
    }
}
```

Factory and `ServiceLoader` registration:

```java
public final class CustomAuthorizationPluginFactory implements AuthorizationPluginFactory {

    @Override
    public String name() {
        return "custom-authz";     // the value of access_controller_type / access_controller.class
    }

    @Override
    public AuthorizationPlugin create(Map<String, String> properties, AuthorizationContext context) {
        return new CustomAuthorizationPlugin(context);
    }
}
```

`src/main/resources/META-INF/services/org.apache.doris.authorization.spi.AuthorizationPluginFactory`:

```text
com.example.authz.CustomAuthorizationPluginFactory
```

## Worked example

The snippets above are elided. A complete source — one that really governs a running FE — lives in the test
tree, together with the test that installs it from a plugin directory and puts SQL through it:

```text
fe/fe-core/src/test/java/org/apache/doris/authorizationexample/
├── ExampleAuthorizationPluginFactory.java        # the four lines that make a jar a plugin
├── ExampleAuthorizationPlugin.java               # privileges by Doris role, plus one row filter
└── AuthorizationPluginFromDirectoryTest.java     # installs it and checks what SQL then does
```

It is the shortest thing that answers the questions a first plugin runs into: how to decide when the
requirement is not one you recognise, why an instance-wide source needs an administration rule of its own,
and what "returning no row filter" does and does not mean.

## Lifecycle and cost

A plugin is created once and kept. Unlike an authentication attempt, an authorization decision happens many
times within a single statement — planning one query checks every table it reads, and listing what a user may
see checks every object that exists — so a source that caches policies has to be the same instance
throughout. The engine builds a new one only when what configures it changes, and calls `close()` on the old
one.

The engine adds no caching of its own and cannot: it does not know when a policy changed. Whatever caching an
external source needs belongs inside the plugin, where it can be invalidated on that source's own terms.

## Packaging and installation

Lay the plugin out one plugin per subdirectory of `authorization_plugins_dir` (default
`${DORIS_HOME}/plugins/authorization`):

```text
plugins/authorization/
└── custom-authz/
    ├── custom-authz-1.0.jar          # the plugin: factory, plugin, service descriptor
    └── lib/
        └── some-dependency.jar       # whatever it needs, isolated from FE's own classpath
```

Then name it in `fe.conf` (`access_controller_type = custom-authz`) to govern the whole instance, or in a
catalog property (`"access_controller.class" = "custom-authz"`) to govern one external catalog.

Each plugin gets its own child-first classloader, so its dependencies do not collide with the FE's. The
exception is `org.apache.doris.authorization.*` — the api and spi types are always loaded from the FE, so
that the types crossing the boundary exist exactly once. Do not bundle this module in your plugin jar.

## Plugin API version

Every plugin jar must declare, in its MANIFEST, the authorization plugin API it was built against:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-jar-plugin</artifactId>
  <configuration>
    <archive>
      <manifestEntries>
        <Doris-Authorization-Plugin-Api-Version>1.0</Doris-Authorization-Plugin-Api-Version>
      </manifestEntries>
    </archive>
  </configuration>
</plugin>
```

The FE admits a plugin whose **major** equals the one it serves; minor and patch are ignored. A jar that
declares nothing is refused, so a plugin written with no awareness of this contract cannot slip through. The
version the FE serves is recorded in `META-INF/doris/authorization-plugin-api-version.properties` inside this
module's jar.

Major is bumped for **any** change to the frozen surface — adding a method or an enum constant just as much
as removing one. That is deliberate: one more `ResourceKind` constant turns every deployed plugin's "a kind I
do not recognise" branch into a refusal of something that used to be allowed. `AuthorizationPluginSurfaceTest`
freezes that surface and fails when it moves.

This check is a compatibility control, not a security one: a plugin can declare a version it was not built
against. What it prevents is a plugin built for another release loading silently and deciding wrongly.

## Test

```bash
mvn -o -f fe/pom.xml -pl fe-authorization/fe-authorization-spi -am test
```
