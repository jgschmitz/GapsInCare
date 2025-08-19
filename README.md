screenshot shows the collection only has the _id index. With a query like

```
{ tenantId: <X>, _id: { $in: [...] } } 
```

the server does many _id seeks then FETCHes each document, which is why the 4k-ID call is slow.
Proposed change (keeps your same query):
Add a targeted, covering index so we can skip the FETCH stage:
```
db.provider_specialities.createIndex(
  { tenantId: 1, _id: 1, neededFieldA: 1 },   // include the exact fields you return
  { name: "byTenant_id_cover_neededFieldA" }
)
```
In the app, project only those fields:
```
q.fields().include("_id").include("neededFieldA");  // keep it tight
q.cursorBatchSize(providerIds.size());              // e.g., 4000
```

Keep wire compression enabled: 
```
?compressors=zstd,snappy.
```
What you should see in explain("executionStats"):
SINGLE_SHARD with one shard.
```
Winning plan: IXSCAN only (no FETCH).
totalKeysExamined ≈ number of IDs; totalDocsExamined = 0 (covered).
Lower executionTimeMillis.
```
If this endpoint needs many different fields (so the index would get wide), a better long-term option is a small read-optimized collection 
(only the fields this API returns) kept in sync via your pipeline.
