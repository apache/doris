{% comment %}
Generates a description of a release.
Parameters:
  releaseName - the name of the release (eg. 1.2.0)
{% endcomment %}

{% assign releaseData = site.data.releases[releaseName] %}
{% if releaseData["state"] == "archived" %}
  {% assign mirror = site.dist_archive %}
  {% assign direct = site.dist_archive %}
{% else %}
  {% assign mirror = site.dist_mirror %}
  {% assign direct = site.dist %}
{% endif %}

* Released: {{ releaseData["date"] | date: "%-d %B %Y" }}
* Source code: [{{ releaseData["tar"] }}]({{mirror}}/orc-{{releaseName}}/{{releaseData["tar"]}})
* [GPG Signature]({{direct}}/orc-{{releaseName}}/{{releaseData["tar"]}}.asc)
  signed by [{{releaseData["signed-by"]}}]({{site.dist}}/KEYS)
* Git tag: [rel/release-{{releaseName}}]({{site.tag_url}}/release-{{releaseName}})
* Maven Central: [ORC {{releaseName}}](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.orc%22%20AND%20v%3A%22{{releaseName}}%22)
* SHA 256: [{{releaseData["sha256"] | truncate: 19}}]({{direct}}/orc-{{releaseName}}/{{releaseData["tar"]}}.sha256)
* Fixed issues: [ORC-{{releaseName}}](https://issues.apache.org/jira/sr/jira.issueviews:searchrequest-printable/temp/SearchRequest.html?jqlQuery=project+%3D+ORC+AND+status+%3D+Closed+AND+fixVersion+%3D+%22{{releaseName}}%22&tempMax=500)

