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
