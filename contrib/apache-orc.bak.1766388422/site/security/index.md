---
layout: page
title: Security
---

Apache ORC is a library rather than an execution framework and thus
is less likely to have security vulnerabilities. However, if you have
discovered one, please follow the process below.

## Reporting a Vulnerability

We strongly encourage folks to report security vulnerabilities to our
private security mailing list first, before disclosing them in a
public forum.

Please note that the security mailing list should only be used for
reporting undisclosed security vulnerabilities in Apache ORC and
managing the process of fixing such vulnerabilities. We cannot accept
regular bug reports or other security related queries at this
address. All mail sent to this address that does not relate to an
undisclosed security problem in Apache ORC will be ignored.

The ORC security mailing list address is:
<a href="mailto:security@orc.apache.org">security@orc.apache.org</a>.
This is a private mailing list and only members of the ORC project
are subscribed.

Please note that we do not use a team GnuPG key. If you wish to
encrypt your e-mail to security@orc.apache.org then please use the GnuPG
keys from [ORC GPG keys](https://people.apache.org/keys/group/orc.asc) for
the members of the
[ORC PMC](https://people.apache.org/phonebook.html?ctte=orc).

## Vulnerability Handling

An overview of the vulnerability handling process is:

* The reporter sends email to the project privately.
* The project works privately with the reporter to resolve the vulnerability.
* The project releases a new version that includes the fix.
* The vulnerability is publicly announced via a [CVE](https://cve.mitre.org/) to the mailing lists and the original reporter.

The full process can be found on the
[Apache Security Process](https://www.apache.org/security/committers.html#vulnerability-handling) page.

## Fixed CVEs

* [CVE-2018-8015](CVE-2018-8015) - ORC files with malformed types cause stack overflow.