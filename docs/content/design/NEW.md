---
layout: doc_page
---

What is Druid?
==============

Druid is designed for real-time slice-and-dice analytics ("OLAP"-style) on large data sets. Druid's key features are a
column-oriented storage layout, a distributed shared-nothing architecture, and ability to generate and leverage indexing
and caching structures. Druid is typically deployed in clusters of tens to hundreds of nodes, and has the ability to
load data from Apache Kafka and Apache Hadoop, among other data sources. Druid offers two query languages: a [SQL
dialect](../querying/sql.html) and a [JSON-over-HTTP API](../querying/native.html).

Druid is a high-performance, column-oriented, distributed data store. What we mean by this is:

- "high performance": Druid aims to provide low query latency and high ingest rates.
- "column-oriented": Druid stores data in a column-oriented format, like other systems designed for
  high-performance analytics. It can also store indexes along with the columns.
- "distributed": Druid is deployed in clusters, typically of tens to hundreds of nodes.
- "data store": Druid loads your data and stores a copy of it on the cluster's local disks (and may cache it
  in memory). It doesn't query your data directly from some other storage system.

Druid was originally developed to power a slice-and-dice analytical UI built on top of large event streams. The original
use case for Druid targeted ingest rates of millions of records/sec, retention of over a year of data, and query
latencies of sub-second to a few seconds. Many people can benefit from such capability, and many already have (see
http://druid.io/druid-powered.html). In addition, new use cases have emerged since Druid's original development, such as
OLAP acceleration of data warehouse tables and more highly concurrent applications operating with relatively narrower
queries.

Druid would typically be used in lieu of more general purpose query systems like Hadoop MapReduce or Spark when query
latency is of the utmost importance. Druid is often used as a data store for powering GUI analytical applications.

Druid is a high-performance, column-oriented, distributed data store. What we mean by this is:

- "high performance": Druid aims to provide low query latency and high ingest rates.
- "column-oriented": Druid stores data in a column-oriented format, like other systems designed for
  high-performance analytics. It can also store indexes along with the columns.
- "distributed": Druid is deployed in clusters, typically of tens to hundreds of nodes.
- "data store": Druid loads your data and stores a copy of it on the cluster's local disks (and may cache it
  in memory). It doesn't query your data from some other storage system.

Druid is a system built to power fast slice-and-dice ("OLAP"-style) queries on large datasets.

It was designed with the intent of being a service and maintaining 100% uptime in the face of code deployments, machine
failures and other eventualities of a production system. It can be useful for back-office use cases as well, but design
decisions were made explicitly targeting an always-up service.

Druid currently allows for single-table queries in a similar manner to [Dremel](http://research.google.com/pubs/pub36632.html) and [PowerDrill](http://www.vldb.org/pvldb/vol5/p1436_alexanderhall_vldb2012.pdf). It adds to the mix

1.  columnar storage format for partially nested data structures
2.  hierarchical query distribution with intermediate pruning
3.  indexing for quick filtering
4.  realtime ingestion (ingested data is immediately available for querying)
5.  fault-tolerant distributed architecture that doesn’t lose data

As far as a comparison of systems is concerned, Druid sits in between PowerDrill and Dremel on the spectrum of functionality. It implements almost everything Dremel offers (Dremel handles arbitrary nested data structures while Druid only allows for a single level of array-based nesting) and gets into some of the interesting data layout and compression methods from PowerDrill.

Druid is a good fit for products that require real-time data ingestion of a single, large data stream. Especially if you are targeting no-downtime operation and are building your product on top of a time-oriented summarization of the incoming data stream. When talking about query speed it is important to clarify what "fast" means: with Druid it is entirely within the realm of possibility (we have done it) to achieve queries that run in less than a second across trillions of rows of data.


Druid uses deep storage only as a backup of your data and as a way to transfer data in the background between Druid
nodes. To respond to queries, Historical nodes do not read from deep storage, but instead read pre-fetched segments
from their local disks before any queries are served. This means that Druid never needs to access deep storage during
a query, helping it offer the best query latencies possible. It also means that you must have enough disk space both
in deep storage and across your Historical nodes for the data you plan to load.

