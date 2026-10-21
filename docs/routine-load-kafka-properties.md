# Routine Load Kafka property validation

Kafka `property.*` options are checked against an explicit allowlist in both FE
and BE. Unknown options are rejected instead of being forwarded to librdkafka.
The shared source of truth is `KAFKA_ALLOWED_PROPERTIES`,
`KAFKA_FILE_PROPERTIES`, and `KAFKA_DORIS_PROPERTIES` in
[`gensrc/thrift/BackendService.thrift`](../gensrc/thrift/BackendService.thrift).
New librdkafka options require review before they can be exposed through SQL.

Consumer identity, fetch tuning, offset isolation, TLS, SASL PLAIN/SCRAM,
OAuth bearer authentication, and Doris AWS MSK IAM options remain supported
through the listed properties. Both `property.sasl.mechanism` and
`property.sasl.mechanisms` are accepted. Existing `aws.*` and `property.aws.*`
spellings are supported for the listed AWS options. Java-only properties such
as `sasl.jaas.config` are rejected; use librdkafka's SASL properties instead.

## Certificates

Certificate/key/CA/CRL/keystore path options must use `FILE:<name>` referencing a
file registered with `CREATE FILE` in the job's database and the `kafka`
catalog. Raw absolute or relative paths on a BE are rejected. FE resolves the
registered file to `FILE:<id>:<md5>`; BE downloads and resolves that managed file
before passing its local path to librdkafka. Inline PEM properties remain
available where listed in the allowlist.

Executable module settings, including `plugin.library.paths`, `ssl.engine.id`,
`ssl.engine.location`, and `ssl.providers`, are rejected even when their values
use `FILE:`.

## Kerberos

Routine Load no longer runs librdkafka's internal `kinit` command. The
`sasl.kerberos.kinit.cmd` and `sasl.kerberos.keytab` properties are rejected.
`sasl.kerberos.min.time.before.relogin` accepts only the literal value `0` and
is also forced to `0` by Doris. This disables both initial ticket acquisition
and periodic renewal by librdkafka, including its default command.

For GSSAPI authentication, operators must provision and renew a Kerberos ticket
cache accessible to the BE process on every BE eligible to run the job. The
principal and service-name properties remain available. Jobs relying on
per-job keytabs or shell commands must migrate to operator-managed tickets
before upgrading.

## Upgrade behavior

- FE validates both CREATE and ALTER statements before forwarding properties.
- Persisted job properties are validated again before file conversion and
  metadata requests. Jobs containing rejected properties fail with an error
  identifying the property; remove the incompatible configuration and recreate
  the job. ALTER merges properties and cannot remove an existing rejected key.
- FE explicitly sends the zero Kerberos refresh interval to protect requests
  sent to older BEs during a rolling upgrade.
- Upgraded BEs validate the complete property map before applying any custom
  setting, covering metadata probes and tasks from older FEs. They disable
  internal Kerberos commands even when the FE did not send the interval.
- Both sides should be upgraded. An old FE sending work to an old BE retains
  its previous behavior until one of those components is upgraded.

No Thrift field IDs, enum values, or persisted data formats change.
