---
layout: doc_page
---

# Ingestion

## Overview

The central concept in Druid is the _segment_. Segme

Druid supports a variety of data ingestion methods, also called indexing methods. In general, with any indexing method,
Druid MiddleManagers are responsible for loading data from some external location and then create immutable
[segments](../design/segments.html) in your [deep storage](../dependencies/deep-storage.html). These segments may be
created on the MiddleManager servers themselves, or, in the case of Hadoop-based ingestion, will be created by Hadoop
MapReduce jobs running on YARN.

## Ingestion methods

In most ingestion methods, this work is done by Druid
MiddleManager nodes. One exception is Hadoop-based ingestion, where this work is instead done using a Hadoop MapReduce
job on YARN (although MiddleManager nodes are still involved in starting and monitoring the Hadoop jobs).

Once segments have been generated and stored in deep storage, they will be loaded by Druid Historical nodes. Some Druid
ingestion methods additionally support _real-time queries_, meaning you can query in-flight data on MiddleManager nodes
before it is finished being converted and written to deep storage. In general, a small amount of data will be in-flight
on MiddleManager nodes relative to the larger amount of historical data being served from Historical nodes.

See the [Design](design.html) page for more details on how Druid stores and manages your data.

The table below lists Druid's most common data ingestion methods, along with comparisons to help you choose
the best one for your situation.

|Method|How it works|Can append and overwrite?|Can handle late data?|Exactly-once ingestion?|Real-time queries?|
|------|------------|-------------------------|---------------------|-----------------------|------------------|
|[Hadoop](hadoop.html)|Druid launches Hadoop Map/Reduce jobs to load data files.|Append or overwrite|Yes|Yes|No|
|[Native batch](native-batch.html)|Druid loads data directly from S3, HDFS, NFS, or other networked storage.|Append or overwrite|Yes|Yes|No|
|[Kafka indexing service](../development/extensions-core/kafka-ingestion.html)|Druid reads directly from Kafka.|Append only|Yes|Yes|Yes|
|[Tranquility](stream-push.html)|You use Tranquility, a client side library, to push individual records into Druid.|Append only|No - late data is dropped|No - may drop or duplicate data|Yes|

## Partitioning

Druid is a distributed data store, and it partitions your data in order to process it in parallel. Druid
[datasources](../design/index.html) are always partitioned first by time based on the
[segmentGranularity](specs.html#granularity) parameter of your ingestion spec. Each of these time partitions is called
a _time chunk_, and each time chunk contains one or more [segments](../design/segments.html). The segments within a
particular time chunk may be partitioned further using options that vary based on the ingestion method you have chosen.

 * With [Hadoop](hadoop.html) you can do hash- or range-based partitioning on one or more columns.
 * With [Native batch](native-batch.html) you can partition on a hash of all dimension columns. This is useful when
 rollup is enabled, since it maximizes your space savings.
 * With [Kafka indexing](../development/extensions-core/kafka-ingestion.html), partitioning is based on Kafka
 partitions, and is not configurable through Druid. You can configure it on the Kafka side by using the partitioning
 functionality of the Kafka producer.
 * With [Tranquility](stream-push.html), partitioning is done by default on a hash of all dimension columns in order
 to maximize rollup. You can also provide a custom Partitioner class; see the
 [Tranquility documentation(https://github.com/druid-io/tranquility/blob/master/docs/overview.md#partitioning-and-replication)
 for details.

All Druid datasources are partitioned by time. Each data ingestion method must acquire a write lock on a particular
time range when loading data, so no two methods can operate on the same time range of the same datasource at the same
time. However, two data ingestion methods _can_ operate on different time ranges of the same datasource at the same
time. For example, you can do a batch backfill from Hadoop while also doing a real-time load from Kafka, so long as
the backfill data and the real-time data do not need to be written to the same time partitions. (If they do, the
real-time load will take priority.)

## Rollup



## Inserts, overwrites, and deletes

