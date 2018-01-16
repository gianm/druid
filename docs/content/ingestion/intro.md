---
layout: doc_page
---

# Data ingestion: overview

## Concepts

Druid supports a variety of data ingestion methods, also called _indexing_ methods. In general, with any method, Druid
will load data from some external location, convert it into Druid's columnar format, and then store it in immutable
_segments_ in your [deep storage](../dependencies/deep-storage.html). In most ingestion methods, this work is done by
Druid MiddleManager nodes. One exception is Hadoop-based ingestion, where this work is instead done using a Hadoop
MapReduce job on YARN (although MiddleManager nodes are still involved in starting and monitoring the Hadoop jobs).

Once segments have been generated and stored in deep storage, Druid Historical nodes load them and serve queries over
them. Some Druid ingestion methods additionally support _real-time queries_, meaning you can query in-flight data
on MiddleManager nodes before it is finished being converted and written to deep storage. In general, a small amount
of data will be in-flight on MiddleManager nodes relative to the larger amount of historical data being served from
Historical nodes.

All Druid datasources are partitioned by time. Each data ingestion method must acquire a write lock on a particular
time range when loading data, so no two methods can operate on the same time range of the same datasource at the same
time. However, two data ingestion methods _can_ operate on different time ranges of the same datasource at the same
time. For example, you can do a batch backfill from Hadoop while also doing a real-time load from Kafka, so long as
the backfill data and the real-time data do not need to be written to the same time partitions. (If they do, the
real-time load will take priority.)

It is important to recognize that Druid uses deep storage only as a backup of your data and as a way to transfer data in
the background between Druid nodes. To respond to queries, Historical nodes do not read from deep storage, but instead
read pre-fetched segments from their local disks before any queries are served. This means that Druid never needs to
access deep storage during a query, helping it offer the best query latencies possible. It also means that you must
have enough disk space both in deep storage and across your Historical nodes for the data you plan to load.

See the [Design](../design/design.html) page for more details on how Druid stores and manages your data.

## Common methods

Druid's most common data ingestion methods are listed in the table below, along with comparisons to help you choose
the best one for your situation.

|Method|How it works|Can append and overwrite?|Can handle late data?|Exactly-once ingestion?|Real-time queries?|
|------|------------|-------------------------|---------------------|-----------------------|------------------|
|[Hadoop](hadoop.html)||Append or overwrite|Yes|Yes|No|
|[Native batch](native-batch.html)||Append or overwrite|Yes|Yes|No|
|[Kafka indexing service](../development/extensions-core/kafka-ingestion.html)|Druid reads from Kafka in an exactly-once, scalable manner.|Append only|Yes|Yes|Yes|
|[Tranquility](stream-push.html)|An external system uses Tranquility, a client side library, to push individual records into Druid.|No - late data is dropped|No - may drop or duplicate data|Yes|
