The New Features and Notable Changes of ORC 1.8:

- [ORC-450]({{site.jira}}/ORC-450) Support selecting list indices without materializing list items
- [ORC-824]({{site.jira}}/ORC-824) Add column statistics for List and Map
- [ORC-1004]({{site.jira}}/ORC-1004) Java ORC writer supports the selection vector
- [ORC-1075]({{site.jira}}/ORC-1075) Support reading ORC files with no column statistics
- [ORC-1125]({{site.jira}}/ORC-1125) Support decoding decimals in RLE
- [ORC-1136]({{site.jira}}/ORC-1136) Optimize reads by combining multiple reads without significant separation into a single read
- [ORC-1138]({{site.jira}}/ORC-1138) Seek vs Read Optimization
- [ORC-1172]({{site.jira}}/ORC-1172) Add row count limit config for one stripe
- [ORC-1212]({{site.jira}}/ORC-1212) Upgrade protobuf-java to 3.17.3
- [ORC-1220]({{site.jira}}/ORC-1220) Set min.hadoop.version to 2.7.3
- [ORC-1248]({{site.jira}}/ORC-1248) Redefine Hadoop dependency for Apache ORC 1.8.0
- [ORC-1256]({{site.jira}}/ORC-1256) Publish test-jar to maven central
- [ORC-1260]({{site.jira}}/ORC-1260) Publish shaded-protobuf classifier artifacts