# Doris FE Authentication SPI

## Overview

`fe-authentication-spi` defines the plugin contract for authentication in Doris FE.

Plugin authors implement:
- `AuthenticationPlugin`
- `AuthenticationPluginFactory`

Plugins are discovered via Java `ServiceLoader`.

## Core Interfaces

### `AuthenticationPlugin`

```java
public interface AuthenticationPlugin extends Plugin {
    String name();

    default String description() { ... }

    boolean supports(AuthenticationRequest request);

    default boolean requiresClearPassword() { return false; }

    default String normalizeBoundIdentity(String rawIdentity) throws AuthenticationException { return rawIdentity; }

    default boolean requiresBoundIdentity() { return false; }

    default boolean supportsMultiStep() { return false; }

    AuthenticationResult authenticate(
        AuthenticationRequest request,
        AuthenticationIntegration integration
    ) throws AuthenticationException;

    default void validate(AuthenticationIntegration integration) throws AuthenticationException { }

    default void initialize(AuthenticationIntegration integration) throws AuthenticationException { }

    default void reload(AuthenticationIntegration integration) throws AuthenticationException {
        initialize(integration);
    }

    default void close() { }
}
```

Result/exception contract:
- Expected auth failures (wrong password, unknown user) should return `AuthenticationResult.failure(...)`.
- Throw `AuthenticationException` for internal/plugin errors (misconfiguration, dependency outage, etc.).

### `AuthenticationPluginFactory`

```java
public interface AuthenticationPluginFactory extends PluginFactory {
    String name();
    AuthenticationPlugin create();
}
```

Factory guidance in this repository:
- One `AuthenticationIntegration` maps to one plugin instance.
- `create()` should return a new plugin object.

## Minimal Plugin Example

```java
public final class CustomAuthPlugin implements AuthenticationPlugin {
    @Override
    public String name() {
        return "custom-auth";
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        return CredentialType.OAUTH_TOKEN.equalsIgnoreCase(request.getCredentialType());
    }

    @Override
    public AuthenticationResult authenticate(
            AuthenticationRequest request,
            AuthenticationIntegration integration) throws AuthenticationException {
        byte[] credential = request.getCredential();
        if (credential == null || credential.length == 0) {
            return AuthenticationResult.failure("Token is required");
        }

        boolean ok = validateToken(credential, integration);
        if (!ok) {
            return AuthenticationResult.failure("Invalid token");
        }

        return AuthenticationResult.success(
                BasicPrincipal.builder()
                        .name(request.getUsername())
                        .authenticator(integration.getName())
                        .build());
    }

    private boolean validateToken(byte[] token, AuthenticationIntegration integration) {
        return true;
    }
}
```

Factory and ServiceLoader registration:

```java
public final class CustomAuthPluginFactory implements AuthenticationPluginFactory {
    @Override
    public String name() {
        return "custom-auth";
    }

    @Override
    public AuthenticationPlugin create() {
        return new CustomAuthPlugin();
    }
}
```

`src/main/resources/META-INF/services/org.apache.doris.authentication.spi.AuthenticationPluginFactory`:

```text
com.example.auth.CustomAuthPluginFactory
```

## Lifecycle

1. Discover factory via `ServiceLoader`
2. `create()` plugin instance
3. `validate()` and `initialize()` with integration config
4. `supports()` + `authenticate()` for requests
5. `close()` when integration cache is evicted or manager cache is cleared

V1 runtime notes:
- External plugin runtime currently supports load only.
- External plugin reload/unload is not supported by the current manager/runtime.
- `reload()` remains an optional SPI hook for future/runtime-specific use, but is not automatically invoked.

## Test

```bash
cd fe-authentication-spi
mvn test
```

## User bindings

A Doris `user@host` account can select one authentication integration and optionally
store its identity in that external system:

```sql
CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION corp_ldap;
ALTER USER 'etl'@'10.%' IDENTIFIED WITH AUTHENTICATION INTEGRATION iam_prod
    AS 'arn:aws:iam::123456789012:role/etl-role';
ALTER USER 'etl'@'10.%' IDENTIFIED BY 'local-password';
```

The integration must exist. `normalizeBoundIdentity` validates and normalizes the
`AS` value during CREATE/ALTER USER execution; it also receives `null` when AS is
omitted. `requiresBoundIdentity` rejects an omitted identity when the plugin needs
one. Neither hook runs during journal replay or image loading. On successful
login, the kernel compares a stored identity exactly against the principal's
`getExternalPrincipal()`. Plugins must normalize their login principals using the
same rules as their DDL hook.

The first account matching the username and source host determines the route.
A binding is exclusive: local passwords, legacy LDAP, the configured primary
plugin, authentication-chain fallback, certificate-only authentication and JIT
users cannot authenticate that account. Plugins requiring clear text credentials
are refused before requesting credentials unless the MySQL channel has completed
TLS. Certificate requirements remain additional requirements. HTTP Basic,
Flight password authentication and stream-load password/certificate authentication
reject bound accounts; these adapters do not negotiate integration credentials.

Root and admin cannot be bound. SET PASSWORD and local password policies are
rejected for bound accounts. Binding clears local password policies and history,
reports a warning when an existing account is bound, and preserves its manual
ACCOUNT_LOCK. ACCOUNT_LOCK and ACCOUNT_UNLOCK remain effective. An ALTER without
IDENTIFIED preserves the binding; a new binding without AS clears the old identity.
An integration with bound users cannot be dropped.

SHOW CREATE USER emits the binding and its normalized identity. SHOW GRANTS adds
AuthIntegration after RequireSan and reports Password as No. The mysql.user
compatibility table exposes the integration in plugin and the identity in
authentication_string.

Bound accounts also retain a random, undisclosed local password hash and a
compatibility account-lock flag. Current FEs distinguish that flag from the
manual lock. Older FEs that support ACCOUNT_LOCK and enforce it for all
authentication paths ignore the new fields and refuse the account. Before rolling
back, disable JIT users and any external/certificate path that bypasses that lock.
Both images
and binding journal records retain that lock. An old-version checkpoint discards
the binding and the distinction between the two locks; after upgrading again,
an administrator must rebuild the binding and explicitly unlock the account.
No FE metadata-version bump is needed.
