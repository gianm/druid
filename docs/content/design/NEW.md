---
layout: doc_page
---

Druid uses deep storage only as a backup of your data and as a way to transfer data in the background between Druid
nodes. To respond to queries, Historical nodes do not read from deep storage, but instead read pre-fetched segments
from their local disks before any queries are served. This means that Druid never needs to access deep storage during
a query, helping it offer the best query latencies possible. It also means that you must have enough disk space both
in deep storage and across your Historical nodes for the data you plan to load.

