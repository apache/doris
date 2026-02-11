# Doris FE Authentication SPI

## Overview

The Authentication SPI (Service Provider Interface) module defines the contract for building authentication plugins. Plugin developers implement these interfaces to add custom authentication methods to Doris FE.

## Purpose

- **Plugin Contract**: Define interfaces for authentication plugins
- **Plugin Discovery**: Enable ServiceLoader-based plugin discovery
- **Extensibility**: Allow third-party authentication integrations
- **Isolation**: Separate plugin API from implementation details

## Key Interfaces

### 1. AuthenticationPlugin

Core interface that all authentication plugins must implement.

```java
public interface AuthenticationPlugin {
    /**
     * Returns the unique name of this plugin
     */
    String name();

    /**
     * Initialize the plugin with configuration
     */
    void initialize(AuthenticationIntegration integration) throws AuthenticationException;

    /**
     * Check if this plugin supports the given request
     */
    boolean supports(AuthenticationRequest request);

    /**
     * Authenticate the user
     */
    AuthenticationResult authenticate(
        AuthenticationRequest request,
        AuthenticationIntegration integration
    ) throws AuthenticationException;

    /**
     * Optional: Clean up resources
     */
    default void destroy() {
        // No-op by default
    }
}
```

### 2. AuthenticationPluginFactory

Factory interface for creating plugin instances via ServiceLoader.

```java
public interface AuthenticationPluginFactory {
    /**
     * Returns the plugin type name
     */
    String name();

    /**
     * Create a new plugin instance
     */
    AuthenticationPlugin create();
}
```

### 3. AuthenticationResult

Result object returned by authentication plugins.

```java
public class AuthenticationResult {
    // Factory methods
    public static AuthenticationResult success(Principal principal);
    public static AuthenticationResult success(Principal principal, Identity identity);
    public static AuthenticationResult failure(String reason);

    // Accessors
    public boolean isSuccess();
    public Principal getPrincipal();
    public Identity getIdentity();
    public String getFailureReason();
}
```

### 4. AuthenticationException

Exception type for authentication errors.

```java
public class AuthenticationException extends Exception {
    public AuthenticationException(String message);
    public AuthenticationException(String message, Throwable cause);

    // Predefined error codes
    public static final String INVALID_CREDENTIALS = "INVALID_CREDENTIALS";
    public static final String USER_NOT_FOUND = "USER_NOT_FOUND";
    public static final String ACCOUNT_LOCKED = "ACCOUNT_LOCKED";
    public static final String CONFIGURATION_ERROR = "CONFIGURATION_ERROR";
}
```

## Creating a Plugin

### Step 1: Implement AuthenticationPlugin

```java
package com.example.auth;

import org.apache.doris.authentication.spi.*;
import org.apache.doris.authentication.*;

public class CustomAuthPlugin implements AuthenticationPlugin {

    private String apiEndpoint;
    private String apiKey;

    @Override
    public String name() {
        return "custom-auth";
    }

    @Override
    public void initialize(AuthenticationIntegration integration)
            throws AuthenticationException {
        // Extract configuration
        this.apiEndpoint = integration.getProperty("api.endpoint");
        this.apiKey = integration.getProperty("api.key");

        // Validate configuration
        if (apiEndpoint == null || apiKey == null) {
            throw new AuthenticationException(
                "Missing required configuration: api.endpoint or api.key",
                AuthenticationException.CONFIGURATION_ERROR
            );
        }
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        // Only support OAuth tokens
        return request.getCredentialType() == CredentialType.OAUTH_TOKEN;
    }

    @Override
    public AuthenticationResult authenticate(
            AuthenticationRequest request,
            AuthenticationIntegration integration)
            throws AuthenticationException {

        String token = new String(request.getCredential());

        try {
            // Validate token with external API
            UserInfo userInfo = validateTokenWithApi(token);

            if (userInfo == null) {
                return AuthenticationResult.failure("Invalid token");
            }

            // Create principal
            Principal principal = BasicPrincipal.builder()
                .name(userInfo.getUsername())
                .authenticator(integration.getName())
                .externalId(userInfo.getUserId())
                .build();

            // Create identity with group information
            Identity identity = Identity.builder()
                .username(userInfo.getUsername())
                .displayName(userInfo.getDisplayName())
                .email(userInfo.getEmail())
                .externalGroups(userInfo.getGroups())
                .build();

            return AuthenticationResult.success(principal, identity);

        } catch (IOException e) {
            throw new AuthenticationException(
                "Failed to validate token: " + e.getMessage(),
                e
            );
        }
    }

    @Override
    public void destroy() {
        // Clean up resources
        // Close HTTP clients, etc.
    }

    private UserInfo validateTokenWithApi(String token) throws IOException {
        // Implementation: Call external API to validate token
        // ...
    }
}
```

### Step 2: Create Plugin Factory

```java
package com.example.auth;

import org.apache.doris.authentication.spi.*;

public class CustomAuthPluginFactory implements AuthenticationPluginFactory {

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

### Step 3: Register via ServiceLoader

Create file: `src/main/resources/META-INF/services/org.apache.doris.authentication.spi.AuthenticationPluginFactory`

Content:
```
com.example.auth.CustomAuthPluginFactory
```

### Step 4: Package and Deploy

```bash
# Build the plugin JAR
mvn clean package

# Deploy to Doris FE
cp target/custom-auth-plugin-1.0.0.jar $DORIS_HOME/fe/plugins/
```

## Plugin Lifecycle

1. **Discovery**: PluginManager discovers plugins via ServiceLoader
2. **Registration**: Factory creates plugin instance
3. **Initialization**: `initialize()` called with configuration
4. **Usage**: `supports()` and `authenticate()` called for each request
5. **Cleanup**: `destroy()` called on shutdown

```
┌──────────────┐
│  Discovery   │ ServiceLoader finds factories
└──────┬───────┘
       │
┌──────▼───────┐
│ Registration │ Factory.create() called
└──────┬───────┘
       │
┌──────▼─────────┐
│ Initialization │ Plugin.initialize(config)
└──────┬─────────┘
       │
┌──────▼────────┐
│  Active Use   │ supports() / authenticate()
└──────┬────────┘
       │
┌──────▼────────┐
│   Cleanup     │ Plugin.destroy()
└───────────────┘
```

## Configuration Properties

Plugins receive configuration via `AuthenticationIntegration.getProperties()`:

```java
@Override
public void initialize(AuthenticationIntegration integration) {
    // Read string properties
    String url = integration.getProperty("ldap.url");

    // Read with default
    String timeout = integration.getPropertyOrDefault("timeout", "30");

    // Read boolean
    boolean useTls = Boolean.parseBoolean(
        integration.getPropertyOrDefault("use.tls", "false")
    );

    // Read integer
    int maxRetries = Integer.parseInt(
        integration.getPropertyOrDefault("max.retries", "3")
    );
}
```

## Best Practices

### 1. Validate Configuration Early

```java
@Override
public void initialize(AuthenticationIntegration integration)
        throws AuthenticationException {
    String url = integration.getProperty("api.url");
    if (url == null || url.isEmpty()) {
        throw new AuthenticationException(
            "Missing required property: api.url",
            AuthenticationException.CONFIGURATION_ERROR
        );
    }
}
```

### 2. Check Request Compatibility

```java
@Override
public boolean supports(AuthenticationRequest request) {
    // Check credential type
    if (request.getCredentialType() != CredentialType.OAUTH_TOKEN) {
        return false;
    }

    // Check integration name (if applicable)
    String integrationName = request.getIntegrationName();
    if (integrationName != null && !integrationName.equals("my-oauth")) {
        return false;
    }

    return true;
}
```

### 3. Handle Errors Gracefully

```java
@Override
public AuthenticationResult authenticate(
        AuthenticationRequest request,
        AuthenticationIntegration integration) {

    try {
        // Perform authentication
        return performAuth(request);

    } catch (InvalidCredentialException e) {
        // User error - return failure
        return AuthenticationResult.failure("Invalid credentials");

    } catch (IOException e) {
        // System error - throw exception
        throw new AuthenticationException(
            "Authentication service unavailable",
            e
        );
    }
}
```

### 4. Clean Up Resources

```java
public class MyPlugin implements AuthenticationPlugin {

    private HttpClient httpClient;

    @Override
    public void initialize(AuthenticationIntegration integration) {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    }

    @Override
    public void destroy() {
        // Clean up resources
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
```

### 5. Thread Safety

```java
public class MyPlugin implements AuthenticationPlugin {

    // Immutable configuration - thread-safe
    private volatile String apiUrl;

    // Shared, thread-safe client
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Override
    public AuthenticationResult authenticate(
            AuthenticationRequest request,
            AuthenticationIntegration integration) {

        // authenticate() may be called concurrently
        // Ensure thread-safety!

        // Don't use instance variables for request state
        // Use local variables instead
        String token = new String(request.getCredential());

        // Thread-safe HTTP call
        return validateToken(token);
    }
}
```

## Testing Plugins

### Unit Testing

```java
@Test
void testAuthenticate_Success() throws AuthenticationException {
    // Create plugin
    MyPlugin plugin = new MyPlugin();

    // Configure
    AuthenticationIntegration integration = AuthenticationIntegration.builder()
        .name("test")
        .type("my-plugin")
        .properties(Map.of("api.key", "test-key"))
        .build();

    plugin.initialize(integration);

    // Create request
    AuthenticationRequest request = AuthenticationRequest.builder()
        .username("alice")
        .credentialType(CredentialType.OAUTH_TOKEN)
        .credential("valid-token".getBytes())
        .build();

    // Authenticate
    AuthenticationResult result = plugin.authenticate(request, integration);

    // Verify
    assertTrue(result.isSuccess());
    assertEquals("alice", result.getPrincipal().getName());
}
```

### Integration Testing

```java
@Test
void testAuthenticate_WithRealService() throws Exception {
    // Start mock service
    MockAuthServer server = new MockAuthServer();
    server.start();

    try {
        // Configure plugin
        MyPlugin plugin = new MyPlugin();
        AuthenticationIntegration integration = AuthenticationIntegration.builder()
            .name("test")
            .type("my-plugin")
            .properties(Map.of("api.url", server.getUrl()))
            .build();

        plugin.initialize(integration);

        // Test authentication
        AuthenticationRequest request = AuthenticationRequest.builder()
            .username("alice")
            .credentialType(CredentialType.OAUTH_TOKEN)
            .credential("valid-token".getBytes())
            .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        assertTrue(result.isSuccess());

    } finally {
        server.stop();
    }
}
```

## Testing

Run SPI module tests:

```bash
cd fe-authentication-spi
mvn test
```

Test coverage: 90%+

## Dependencies

**Runtime:**
- fe-authentication-api (core data models)

**Test:**
- JUnit 5
- Mockito

## Examples

See plugin implementations:
- [Password Plugin](../fe-authentication-plugins/fe-authentication-plugin-password/) - Simple password authentication
- [LDAP Plugin](../fe-authentication-plugins/fe-authentication-plugin-ldap/) - External LDAP integration

## License

Licensed under the Apache License 2.0.

---

**Version**: 1.2-SNAPSHOT
**Last Updated**: 2026-02-11
**Maintainers**: Apache Doris Authentication Team
