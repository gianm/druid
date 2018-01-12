---
layout: doc_page
---

# Data ingestion: overview

## Concepts

Druid supports a variety of data ingestion methods, also called _indexing_ methods. In general, with any method, Druid
will load data from some external source, convert it into Druid's columnar format, and then store it in immutable
_segments_ in your [deep storage](../dependencies/deep-storage.html). In most ingestion methods, this work is done by
Druid MiddleManager nodes. One exception is Hadoop-based ingestion, where this work is instead done using a Hadoop
MapReduce job on YARN (although MiddleManager nodes are still involved in starting and monitoring the Hadoop jobs).

Once segments have been generated and stored in deep storage, Druid Historical nodes load them and serve queries over
them. Some Druid ingestion methods additionally support _real-time queries_, meaning you can query in-flight data
on MiddleManager nodes before it is finished being converted and written to deep storage. In general, a small amount
of data will be in-flight on MiddleManager nodes relative to the larger amount of historical data being served from
Historical nodes.

It is important to recognize that even though Druid uses deep storage as a backup of your data, Historical nodes must
prefetch all active segments to their local disks before any queries are served. This means that Druid never needs to
access deep storage during a query, helping it offer the best query latencies possible. It also means that you must
have enough disk space across your Historical nodes for the data you plan to load.

See the [Design](../design/design.html) page for more details on how Druid stores and manages your data.

## Common methods

Druid's most common data ingestion methods are listed below.

- [Hadoop](hadoop.html), where Druid loads data directly from Hadoop using Hadoop jobs on YARN. In this case, Druid
MiddleManagers start Hadoop jobs that
- [Native batch](native-batch.html).
- [Kafka indexing service](../development/extensions-core/kafka-ingestion.html).
- [Tranquility](stream-push.html).

## Comparisons

The table below includes comparisons between common Druid ingestion methods, to help you choose the best one for your
situation.

|Method|How it works|Append or overwrite?|Can handle late data?|Exactly-once guarantees?|Real-time queries?|
|------|------------|--------------------|---------------------|------------------------|------------------|
|Hadoop||Yes|Yes|No|
|Native batch|||Yes|No|
|Kafka indexing service|||Yes|Yes|
|Tranquility|An external system uses Tranquility, a client side library, to write|No - late data is dropped.|No|Yes|
