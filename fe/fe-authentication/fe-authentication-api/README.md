# Doris FE Authentication API

## Overview

The Authentication API module provides the core data models and public interfaces for the Doris FE Authentication Framework. This module defines the contracts and data structures used across all authentication components.

## Purpose

- **Core Data Models**: Define authentication request, response, principal, subject, and identity
- **Public Interfaces**: Provide stable APIs for FE core integration
- **Type Safety**: Strong typing for credentials and authentication metadata
- **Immutability**: Thread-safe immutable data structures using builder pattern

## Key Components

### 1. AuthenticationRequest

Represents an authentication request with credentials and metadata.

```java
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential("password123".getBytes())
    .remoteHost("192.168.1.100")
    .integrationName("ldap_corporate")  // Optional: target specific integration
    .build();
```

**Key Fields:**
- `username`: The user identifier
- `credentialType`: Type of credential (password, token, certificate, etc.)
- `credential`: Raw credential data (byte array)
- `remoteHost`: Client IP address
- `integrationName`: Optional target integration name

### 2. Principal

Represents an authenticated identity.

```java
Principal principal = BasicPrincipal.builder()
    .name("alice")
    .authenticator("ldap_corporate")
    .externalId("uid=alice,ou=users,dc=corp,dc=com")  // Optional
    .build();
```

**Key Fields:**
- `name`: Principal name (username)
- `authenticator`: Name of the authentication integration that authenticated this principal
- `externalId`: Optional external identifier (e.g., LDAP DN, OAuth subject)

### 3. Identity

Represents user identity information from external systems.

```java
Identity identity = Identity.builder()
    .username("alice")
    .displayName("Alice Smith")
    .email("alice@corp.com")
    .externalGroups(Set.of("cn=admins,ou=groups,dc=corp,dc=com"))
    .build();
```

**Key Fields:**
- `username`: User identifier
- `displayName`: Human-readable display name
- `email`: User email address
- `externalGroups`: Set of external group memberships

### 4. Subject

Represents the complete authenticated subject with roles and permissions.

```java
Subject subject = Subject.builder()
    .principal(principal)
    .identity(identity)
    .roles(Set.of("admin", "developer"))
    .remoteIp("192.168.1.100")
    .build();
```

**Key Fields:**
- `principal`: The authenticated principal
- `identity`: User identity information
- `roles`: Set of assigned roles
- `remoteIp`: Client IP address

### 5. AuthenticationIntegration

Configures an authentication integration (plugin instance).

```java
AuthenticationIntegration integration = AuthenticationIntegration.builder()
    .name("ldap_corporate")
    .type("ldap")
    .enabled(true)
    .properties(Map.of(
        "ldap.url", "ldap://ldap.corp.com:389",
        "ldap.base_dn", "dc=corp,dc=com"
    ))
    .build();
```

**Key Fields:**
- `name`: Unique integration name
- `type`: Plugin type identifier
- `enabled`: Enable/disable flag
- `properties`: Configuration key-value pairs

### 6. CredentialType

Enum defining supported credential types:

```java
public enum CredentialType {
    CLEAR_TEXT_PASSWORD,  // Plain text password
    HASHED_PASSWORD,      // Pre-hashed password
    OAUTH_TOKEN,          // OAuth 2.0 access token
    OIDC_TOKEN,           // OpenID Connect ID token
    SAML_ASSERTION,       // SAML 2.0 assertion
    CLIENT_CERTIFICATE,   // X.509 client certificate
    KERBEROS_TICKET       // Kerberos ticket
}
```

### 7. AuthenticationBinding

Binds a user to a specific authentication integration.

```java
AuthenticationBinding binding = new AuthenticationBinding("alice", "ldap_corporate");

String username = binding.getUsername();        // "alice"
String integration = binding.getIntegrationName();  // "ldap_corporate"
```

## Design Patterns

### Builder Pattern

All data models use the builder pattern for construction:

```java
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential("password".getBytes())
    .build();
```

**Benefits:**
- Named parameters for clarity
- Optional fields with sensible defaults
- Validation at build time
- Immutable objects

### Immutability

All data models are immutable after construction:

```java
// Objects are immutable
Principal principal = BasicPrincipal.builder()
    .name("alice")
    .authenticator("ldap")
    .build();

// Cannot modify - no setters
// principal.setName("bob");  // Compile error!

// Create new object instead
Principal newPrincipal = BasicPrincipal.builder()
    .from(principal)
    .name("bob")
    .build();
```

**Benefits:**
- Thread-safe by default
- Safe to share across components
- Predictable behavior
- Easier to reason about

## Usage Examples

### Basic Password Authentication

```java
// Create authentication request
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential("password123".getBytes())
    .remoteHost("192.168.1.100")
    .build();

// Process authentication (in handler/plugin)
Principal principal = BasicPrincipal.builder()
    .name(request.getUsername())
    .authenticator("password")
    .build();

Identity identity = Identity.builder()
    .username(request.getUsername())
    .build();

Subject subject = Subject.builder()
    .principal(principal)
    .identity(identity)
    .roles(Set.of("user"))
    .remoteIp(request.getRemoteHost())
    .build();
```

### LDAP Authentication with External Groups

```java
// Create authentication request
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential("password123".getBytes())
    .remoteHost("192.168.1.100")
    .integrationName("ldap_corporate")
    .build();

// After successful LDAP authentication
Principal principal = BasicPrincipal.builder()
    .name("alice")
    .authenticator("ldap_corporate")
    .externalId("uid=alice,ou=users,dc=corp,dc=com")
    .build();

Identity identity = Identity.builder()
    .username("alice")
    .displayName("Alice Smith")
    .email("alice@corp.com")
    .externalGroups(Set.of(
        "cn=admins,ou=groups,dc=corp,dc=com",
        "cn=developers,ou=groups,dc=corp,dc=com"
    ))
    .build();

Subject subject = Subject.builder()
    .principal(principal)
    .identity(identity)
    .roles(Set.of("admin", "developer"))  // Mapped from groups
    .remoteIp(request.getRemoteHost())
    .build();
```

### OAuth 2.0 Token Authentication

```java
// Create authentication request with OAuth token
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice@corp.com")
    .credentialType(CredentialType.OAUTH_TOKEN)
    .credential("eyJhbGciOiJSUzI1NiIs...".getBytes())
    .remoteHost("192.168.1.100")
    .integrationName("oauth2_google")
    .build();

// After token validation
Principal principal = BasicPrincipal.builder()
    .name("alice@corp.com")
    .authenticator("oauth2_google")
    .externalId("google-oauth2|123456789")
    .build();

Identity identity = Identity.builder()
    .username("alice@corp.com")
    .displayName("Alice Smith")
    .email("alice@corp.com")
    .build();

Subject subject = Subject.builder()
    .principal(principal)
    .identity(identity)
    .roles(Set.of("user"))
    .remoteIp(request.getRemoteHost())
    .build();
```

## Testing

The API module has comprehensive unit tests with 90%+ coverage.

### Run Tests

```bash
cd fe-authentication-api
mvn test
```

### Test Coverage

```bash
mvn test jacoco:report
open target/site/jacoco/index.html
```

### Test Structure

- `BasicPrincipalTest`: Tests Principal implementation (12 tests)
- `AuthenticationRequestTest`: Tests request builder (12 tests)
- `IdentityTest`: Tests Identity model (15 tests)
- `SubjectTest`: Tests Subject model (15 tests)

## Dependencies

**Zero Runtime Dependencies**

This module has no runtime dependencies, ensuring:
- Minimal footprint
- Fast loading
- No dependency conflicts
- Easy integration

**Test Dependencies:**
- JUnit 5 (Jupiter)
- Mockito

## Best Practices

### 1. Always Use Builders

```java
// Good
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential("password".getBytes())
    .build();

// Avoid direct construction if available
// new AuthenticationRequest(...)  // May not be available
```

### 2. Validate Input

```java
// The builder validates at build time
try {
    AuthenticationRequest request = AuthenticationRequest.builder()
        .username(null)  // Invalid!
        .build();
} catch (NullPointerException e) {
    // Handle validation error
}
```

### 3. Handle Credentials Securely

```java
// Good: Clear credentials after use
byte[] credential = "password".getBytes();
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential(credential)
    .build();

// Process authentication...

// Clear sensitive data
Arrays.fill(credential, (byte) 0);
```

### 4. Use Appropriate Credential Types

```java
// For password authentication
request.builder()
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential(password.getBytes(StandardCharsets.UTF_8))
    .build();

// For OAuth tokens
request.builder()
    .credentialType(CredentialType.OAUTH_TOKEN)
    .credential(token.getBytes(StandardCharsets.UTF_8))
    .build();
```

### 5. Preserve Remote IP

```java
// Always include remote IP for audit logging
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential(password)
    .remoteHost("192.168.1.100")  // Important for security
    .build();
```

## API Stability

This module provides **stable public APIs** for:
- FE Core integration
- Plugin development
- External consumers

**Compatibility Promise:**
- Backward compatible changes only
- Deprecation warnings before removal
- Semantic versioning

## License

Licensed under the Apache License 2.0. See LICENSE file.

---

**Version**: 1.2-SNAPSHOT
**Last Updated**: 2026-02-11
**Maintainers**: Apache Doris Authentication Team
