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

Routine Load supports automatic Kerberos login and ticket refresh with a
per-job principal and a Doris-managed keytab:

```sql
"property.security.protocol" = "SASL_SSL",
"property.sasl.mechanism" = "GSSAPI",
"property.sasl.kerberos.service.name" = "kafka",
"property.sasl.kerberos.principal" = "loader@EXAMPLE.COM",
"property.sasl.kerberos.keytab" = "FILE:loader.keytab"
```

Register `loader.keytab` with `CREATE FILE` in the job's database and `kafka`
catalog first. An explicit, nonempty principal and a SASL protocol are required
when a keytab is supplied. Raw BE keytab paths are rejected. The BE still needs
operator-configured Kerberos realm/KDC settings and network access to the KDC.

After resolving the managed file, Doris's native Kerberos ticket manager calls
libkrb5 to acquire tickets and writes the process default credential cache,
selected by the operator's `KRB5CCNAME` or Kerberos configuration. The unmodified
librdkafka/SASL client discovers credentials through its existing default-cache
behavior. Doris does not change `KRB5CCNAME` while jobs are running.

Doris refreshes tickets at half their lifetime or the configured refresh
interval, whichever is shorter. A refresh failure is logged and retried on the
next poll. Consumers retain their native refresh worker while they are alive.
Default-cache writers are serialized, and releasing a worker does not delete the
default cache. Other users of the ticket manager retain its private-cache mode.

This preserves the existing shared-cache limitation: jobs using different
identities on one BE can overwrite the same default cache during login or
refresh. This change does not provide per-job credential isolation. Use a common
Kafka identity for jobs sharing a default cache; multi-identity isolation is
outside the scope of this fix.

`sasl.kerberos.kinit.cmd` remains forbidden. The librdkafka
`sasl.kerberos.min.time.before.relogin` option accepts only the literal `0` and
is forced to `0` by Doris: it disables librdkafka's shell command, **not** Doris's
native renewal worker. No SQL property is interpolated into a shell command.
Users who explicitly configured this interval must remove it or set it to `0`.

Without a keytab, GSSAPI can continue using an operator-managed default ticket
cache; in that mode the operator remains responsible for ticket renewal.

## Upgrade behavior

- FE validates both CREATE and ALTER statements before forwarding properties.
- Persisted job properties are validated again before file conversion and
  metadata requests. Jobs containing rejected properties fail with an error
  identifying the property; remove the incompatible configuration and recreate
  the job. ALTER merges properties and cannot remove an existing rejected key.
- FE explicitly sends the zero Kerberos refresh interval to protect requests
  sent to older BEs during a rolling upgrade. Native automatic login requires
  upgraded BEs; an older BE does not run the native refresh worker. Upgrade
  eligible BEs before relying on managed keytab login.
- Upgraded BEs validate the complete property map before applying any custom
  setting, covering metadata probes and tasks from older FEs. They disable
  internal Kerberos commands even when the FE did not send the interval.
- Both sides should be upgraded. An old FE sending work to an old BE retains
  its previous behavior until one of those components is upgraded.

This fix adds no librdkafka patch or custom librdkafka property and requires no
third-party dependency rebuild beyond the normal Doris build requirements.

No Thrift field IDs, enum values, or persisted data formats change.
