---
layout: doc_page
---

# Data ingestion: overview

<a name="methods" />
## Ingestion methods

Druid supports a variety of data ingestion methods, also called _indexing_ methods. In general, with any method, Druid
will load data from some external location, convert it into Druid's columnar format, and then store it in immutable
_segments_ in your [deep storage](../dependencies/deep-storage.html). In most ingestion methods, this work is done by
Druid MiddleManager nodes. One exception is Hadoop-based ingestion, where this work is instead done using a Hadoop
MapReduce job on YARN (although MiddleManager nodes are still involved in starting and monitoring the Hadoop jobs).

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

Druid [datasources](../design/index.html) are partitioned into immutable _segments_. Each segment

All Druid datasources are partitioned by time. Each data ingestion method must acquire a write lock on a particular
time range when loading data, so no two methods can operate on the same time range of the same datasource at the same
time. However, two data ingestion methods _can_ operate on different time ranges of the same datasource at the same
time. For example, you can do a batch backfill from Hadoop while also doing a real-time load from Kafka, so long as
the backfill data and the real-time data do not need to be written to the same time partitions. (If they do, the
real-time load will take priority.)

## Rollup

<a name="iod" />
## Inserts, overwrites, and deletes

