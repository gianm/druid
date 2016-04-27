---
layout: doc_page
---

# Bitmap Aggregations

Make sure to [include](../../operations/including-extensions.html) `druid-bitmap-aggregation` as an extension.

This extension includes a compressed-bitmap-based aggregator and filter,
implemented using Roaring bitmaps. The aggregator can be used to return
distinct sets or exact distinct counts of 32-bit integer values, as well
as to do set operations (union, intersect, difference) over these sets.
The filter can be used like an "IN" filter that takes a bitmap rather than
a JSON array.

The filter accepts the same kind of bitmaps as the aggregator returns,
so you can use them together to do a join like:

```
SELECT Country.Code FROM Country
WHERE
  Country.Continent = 'Europe'
  AND Country.Id IN (SELECT City.CountryId
                     FROM City
                     WHERE City.Population > 1000000);
```

Assuming CountryId is an integer, this could be done with two queries.
The first query is over `City`, and uses a bitmap aggregator to build
a bitmap of CountryIds for cities with population over a million. The
second query is over `Country`, and uses that bitmap as a filter ANDed
together with another filter on `Continent = 'Europe'`.

## Bitmap Aggregator

```json
{
  "type" : "bitmap",
  "name" : <output_name>,
  "fieldName" : <column_name>,
  "isInputBitmap" : <boolean>
}
```

|Property|Description|Required|
|--------|-----------|--------|
|`isInputBitmap`|true if fieldName is a bitmap metric column, false if fieldName is a string dimension column.|no (default false)|

If `isInputBitmap` is false, the input must be a string column with 32-bit integers
encoded as 0-padded big endian hex, e.g. "0000000A" for decimal 10.

Output in a query will be an integer indicating the size of the bitmap (i.e.
the distinct count of aggregated values). If used in a post-aggregator, instead
of returning a count, this will return a bitmap object.

### Including bitmaps in segments

You can include a bitmap as a metric in a segment. These can be further
aggregated at query time but they cannot currently be filtered on. To do
this, use the "bitmap" aggregator at ingestion time with `"isInputBitmap" : false`.

### Bitmap Size PostAggregator

```json
{
  "type" : "bitmapSize",
  "name" : <output_name>,
  "field" : <other bitmap postaggregator>
}
```

|Property|Description|Required|
|--------|-----------|--------|
|`field`|fieldAccess type post aggregator that accesses a bitmap aggregator or a bitmapSet post aggregator.|yes|

Output will be an integer indicating the size of the bitmap (i.e. the distinct
count of aggregated values).

### Bitmap Set PostAggregator

```json
{
  "type" : "bitmapSet",
  "name" : <output_name>,
  "func" : <UNION|INTERSECT|NOT>,
  "fields" : <other bitmap postaggregators>
}
```

|Property|Description|Required|
|--------|-----------|--------|
|`func`|UNION, INTERSECT, or NOT|yes|
|`fields`|Array of fieldAccess type post aggregators that access bitmap aggregators or other bitmapSet post aggregators. For "UNION" or "INTERSECT" these will all be unioned or intersected. For "NOT", all sets after the first set will be subtracted from the first set.|yes|

Output will be a bitmap object.

### Bitmap Raw PostAggregator

```json
{
  "type" : "bitmapRaw",
  "name" : <output_name>,
  "format" : <LIST|ROARINGBASE64>,
  "field" : <other bitmap postaggregator>
}
```

|Property|Description|Required|
|--------|-----------|--------|
|`format`|LIST or ROARINGBASE64|no (default == LIST)|
|`field`|fieldAccess type post aggregator that accesses a bitmap aggregator or a bitmapSet post aggregator.|yes|

Output will be either a list of integers (with format LIST) or a Base64-encoded
bitmap object (with format ROARINGBASE64).

## Bitmap filter

```json
{
  "type" : "bitmap",
  "dimension" : <column_name>,
  "bitmapBase64" : <Base64 encoded bitmap>,
  "isInputBitmap" : <boolean>
}
```

|Property|Description|Required|
|--------|-----------|--------|
|`dimension`|Dimension to filter.|yes|
|`bitmapBase64`|Base64 encoded bitmap, of the same form returned by the `bitmapRaw` post-aggregator.|yes|
|`isInputBitmap`|true if fieldName is a bitmap metric column, false if fieldName is a string dimension column.|no (default false)|

If `isInputBitmap` is false, the input must be a string column with 32-bit integers
encoded as 0-padded big endian hex, e.g. "0000000A" for decimal 10.

Note that bitmap metric columns are not supported, so setting `isInputBitmap` to
true will cause an error.

## Experimental

This extension is experimental. Its API is subject to change and it has not
yet received extensive testing in production. Additionally, it has some
technical limitations:

- No strict bound on memory use for on-heap aggregations.
- Aggregations are always done on-heap, even for query engines that support
  off-heap aggregations.
- Filters on bitmap columns are not supported, only filters on string columns
  whose values can be used to create bitmaps at query time.
- Only one specific string encoding of integers is supported (0-padded big
  endian hex).
