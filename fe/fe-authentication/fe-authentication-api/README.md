# Doris FE Authentication API

## Overview

`fe-authentication-api` defines the core authentication data model used by protocol adapters,
handler orchestration, and plugins.

This module intentionally stays small and stable:
- No plugin loading logic
- No protocol-specific handshake logic
- No authorization model (`Subject`/`Identity` are deprecated and removed)

## Main Types

### `AuthenticationRequest`

Protocol-agnostic authentication input.

```java
AuthenticationRequest request = AuthenticationRequest.builder()
    .username("alice")
    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
    .credential("password123".getBytes(StandardCharsets.UTF_8))
    .remoteHost("192.168.1.100")
    .remotePort(9030)
    .clientType("mysql")
    .property("trace_id", "req-123")
    .build();
```

Key fields:
- `username`
- `credentialType`
- `credential`
- `remoteHost` / `remotePort`
- `clientType`
- `properties`

### `Principal` and `BasicPrincipal`

Authentication output identity contract.

```java
Principal principal = BasicPrincipal.builder()
    .name("alice")
    .authenticator("corp_ldap")
    .externalPrincipal("uid=alice,ou=users,dc=example,dc=com")
    .addExternalGroup("developers")
    .attribute("email", "alice@example.com")
    .build();
```

Copy from existing principal:

```java
Principal updated = BasicPrincipal.builder(principal)
    .attribute("department", "data")
    .build();
```

### `AuthenticationResult`

Authentication result is state-driven:
- `SUCCESS`
- `CONTINUE`
- `FAILURE`

```java
AuthenticationResult ok = AuthenticationResult.success(principal);
AuthenticationResult needMore = AuthenticationResult.continueWith(state, challenge);
AuthenticationResult failed = AuthenticationResult.failure("Invalid credential");
```

### `AuthenticationIntegration`

A named auth configuration instance.

```java
AuthenticationIntegration integration = AuthenticationIntegration.builder()
    .name("corp_ldap")
    .type("ldap")
    .property("server", "ldap://ldap.example.com:389")
    .property("base_dn", "dc=example,dc=com")
    .comment("Corporate LDAP")
    .build();
```

### `AuthenticationBinding`

User-to-integration binding model.

```java
AuthenticationBinding binding = AuthenticationBinding.forUser("alice", "corp_ldap");
```

### `CredentialType`

Built-in credential type constants (string-based, extensible):
- `MYSQL_NATIVE_PASSWORD`
- `CLEAR_TEXT_PASSWORD`
- `KERBEROS_TOKEN`
- `OAUTH_TOKEN`
- `OIDC_ID_TOKEN`
- `X509_CERTIFICATE`
- `JWT_TOKEN`
- `SAML_ASSERTION`

### `AuthenticationException`

Authentication failure reason object.

Use it in two ways:
- Return expected auth failures via `AuthenticationResult.failure(...)`
- Throw only for internal/plugin errors

## Design Notes

- API objects are immutable after construction.
- `byte[]` fields are carried as-is by design; treat them as sensitive and short-lived.
- Authorization-layer models are intentionally out of this module.

## Test

```bash
cd fe-authentication-api
mvn test
```
