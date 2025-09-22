[README_Mongo_Model.md](https://github.com/user-attachments/files/22431542/README_Mongo_Model.md)
# Flat → MongoDB Document Model

Transform a flat, column-per-attribute dataset (your Spark `StructType`) into a clean, query-friendly MongoDB document model.

---

## Table of Contents

- [Why reshape](#why-reshape)
- [Target identity](#target-identity)
- [Schema overview](#schema-overview)
- [Field mapping](#field-mapping)
- [Type conversion rules](#type-conversion-rules)
- [Example document](#example-document)
- [Transformation steps](#transformation-steps)
- [Writing to MongoDB](#writing-to-mongodb)
- [Indexes](#indexes)
- [Validation checklist](#validation-checklist)

---

## Why reshape

- The flat layout scatters related attributes, making queries and indexes awkward.
- Many fields are string-typed even when they are flags, numbers, or dates.
- Grouping related attributes (member, gap, HCC, labs, rules) as subdocuments and arrays matches read patterns and reduces joins.

---

## Target identity

Use a deterministic `_id` so upserts are idempotent.

```text
_id = "{GLB_MBR_ID}:{GAP_ID}:{SUS_YEAR}"
```

If you have a guaranteed unique, stable key such as `UNIQ_MBR_GAP_ID`, you can use that instead.

---

## Schema overview

Top-level groups in the target document:

- `member` — ids, age, gender, years of service
- `client` — CLI identifiers
- `status` — active/ignore/disenrollment flags
- `engine` — provenance (dates, source, run ids)
- `gap` — id, description, source, status, rank, probabilities; optional `gap.sub`
- `dssCategory`
- `hcc`
- `encounter` — start/end dates
- `labs` — counts, DOS, LOINC codes/types/results (arrays)
- `manifestation`
- `related` — `dx` and `cpt` subdocs
- `rx`
- `provider`
- `recommendation`
- `rules`
- `coverage`
- `counts`
- `flags`
- `sourceFile`
- `suspect`
- `likelihood`, `ttlProb` (top-level numbers)

Field naming tip: prefer lowerCamelCase for MongoDB fields.

---

## Field mapping

Examples of how flat columns map to the document:

| Flat column(s) | Target field |
|---|---|
| `GLB_MBR_ID`, `MBR_ID`, `MBR_AGE`, `GENDER`, `YOS` | `member.globalId`, `member.memberId`, `member.age`, `member.gender`, `member.yearsOfService` |
| `CLI_ID`, `CLI_SK`, `SUB_CLI_SK`, `D_SUB_CLI_SK`, `CLNT_GU_ID` | `client.*` |
| `ACTIVE_FLAG`, `DISENROLLMENT_FLAG`, `IGNORE`, `IGNORE_REASON`, `STS_CHG_IND` | `status.*` |
| `ENGINE_DATE`, `ENGINE_SOURCE`, `ENGINE_TRIGGEREVENT`, `ENGINE_GAP_ID`, `BATCH_ID`, `RUN_ID` | `engine.*` |
| `GAP_*`, `SUB_GAP_*` | `gap.*`, `gap.sub.*` |
| `DSS_CTGY*` | `dssCategory.*` |
| `HCC*`, `SEC_HCC`, `HIERARCHICAL_STATUS` | `hcc.*` |
| `ENCOUNTER_START_DATE`, `ENCOUNTER_END_DATE` | `encounter.start`, `encounter.end` |
| `LAB_*` | `labs.*` with arrays for LOINC/types/results |
| `RELDX_*`, `RELCPT_*`, `RELATE_*` | `related.dx.*`, `related.cpt.*` |
| `RX_*`, `NDC`, `NDC_DESC` | `rx.*` |
| `PROVIDER_ID` | `provider.id` |
| `RECOM_*` | `recommendation.*` |
| `RULE_*`, `PAR_RULE_*` | `rules.*` |
| `MDL_*`, `IND_STD_MDL_CD`, `METAL_LEVEL` | `coverage.*` |
| `*_CNT`, factors | `counts.*` |
| Remaining flags | `flags.*` |
| `SRC_FL_*`, `SRC_SPCT_ID` | `sourceFile.*` |
| `SUSPECT_PERIOD`, `SUS_YEAR` | `suspect.*` |
| `LIKELIHOOD`, `TTL_PROB` | `likelihood`, `ttlProb` |

---

## Type conversion rules

- **Booleans**: columns ending with `_FLAG` and any Y/N, 1/0, TRUE/FALSE strings become booleans.
- **Numbers**: counts (`*_CNT`), ranks (`*_RNK`), factors (`*_FCT`), probabilities (`LIKELIHOOD`, `TTL_PROB`) become numeric types (`int` or `double`).
- **Dates**: `ENCOUNTER_*`, `SRC_FL_DT`, `ENGINE_DATE`, `LAB_DOS` become `ISODate`.
- **IDs and codes**: keep as strings when leading zeros matter (member ids, NDC, LOINC, ICD/HCC, GAP_ID, provider id).
- **Arrays**: fold numbered fields into arrays (for example `LAB_LOINC_CD1/2` → `labs.loinc`).

---

## Example document

```json
{
  "_id": "MBR12345:GAP987:2024",
  "member": { "globalId": "MBR12345", "memberId": "M123", "modId": "MOD1", "age": 67, "gender": "F", "yearsOfService": 12 },
  "client": { "cliId": "C001", "cliSk": "123", "subCliSk": "SUB9", "dSubCliSk": "D1", "clientGuid": "550e8400-e29b-41d4-a716-446655440000" },
  "status": { "active": true, "disenrolled": false, "ignore": false, "ignoreReason": null, "statusChangeInd": "N" },
  "engine": { "date": "2024-12-01T00:00:00Z", "source": "RULESET_X", "triggerEvent": "CLAIM", "engineGapId": "ENG123", "batchId": "BATCH42", "runId": "RUN20241201" },
  "gap": {
    "id": "GAP987", "description": "Statin therapy for diabetes", "source": "RULE", "status": "OPEN",
    "statusChangeReason": "NEW", "reason": "No recent fill", "rank": 3,
    "probability": 0.74, "totalProbability": 0.80, "keyword": "statin",
    "groupId": "GRP1", "grouperId": "GRPR9",
    "sub": { "id": "SG1", "rank": 2, "reason": "A1c>8", "reasonInd": "HBA1C", "status": "OPEN", "frequency": "MONTHLY", "scoreValue": 0.6 }
  },
  "dssCategory": { "code": "DSS1", "desc": "Diabetes", "factor": 1.2, "gapStatus": "OPEN", "source": "RULE" },
  "hcc": { "primary": "HCC18", "secondary": "HCC19", "group": "HCC_GRP_A", "desc": "Diabetes with complications", "factor": 0.32, "gapStatus": "OPEN", "source": "CLAIMS", "hierarchicalStatus": "ACTIVE" },
  "encounter": { "start": "2024-10-10T00:00:00Z", "end": "2024-10-12T00:00:00Z" },
  "labs": { "hasLabs": true, "count": 2, "dos": "2024-09-15T00:00:00Z", "loinc": ["1234-5","6789-0"], "types": ["A1C","LDL"], "results": ["8.2","140"] },
  "manifestation": { "flag": false, "count": 0, "dx": null, "icdVersion": "10" },
  "related": { "dx": { "flag": true, "count": 1, "icdVersion": "10", "codes": ["E11.9"] }, "cpt": { "flag": false, "count": 0, "codes": [] } },
  "rx": { "flag": true, "count": 1, "ndc": "00093-7424", "ndcDesc": "Atorvastatin 20mg" },
  "provider": { "id": "PRV123" },
  "recommendation": { "source": "ENGINE", "type": "MED", "text": "Start or refill statin therapy", "value": "ATORVASTATIN 20MG" },
  "rules": { "group": "CARDIO", "hier": "1", "subHier": "A", "id": "RULE_1001", "masterId": "RM_01", "shortName": "STATIN_DM", "version": "v5" },
  "coverage": { "modelCode": "MDL1", "modelVersion": "2024.1", "industryStdModelCode": "HEDIS", "metalLevel": "GOLD" },
  "counts": { "lab": 2, "rx": 1, "dc": 0, "other": 0, "relatedDx": 1, "relatedCpt": 0, "cpc": 0, "memberTotalDssFactor": 2.1, "memberTotalHccFactor": 0.32 },
  "flags": { "cpc": false, "lab": true, "manif": false, "other": false, "relDx": true, "relCpt": false, "rx": true, "adultChildInd": "ADULT", "disenrollmentFlag": false },
  "sourceFile": { "name": "inbound_2024_10_15.csv", "date": "2024-10-15T00:00:00Z", "spectId": "SPCT1" },
  "suspect": { "period": "2024Q4", "year": 2024 },
  "likelihood": 0.74,
  "ttlProb": 0.80
}
```

---

## Transformation steps

1. Convert types  
   Flags to booleans, counts and probabilities to numbers, date strings to ISODate.

2. Build subdocuments  
   Group related fields into the structures listed in the schema overview.

3. Construct arrays  
   Combine numbered lab fields into arrays and drop empty values.

4. Create a deterministic `_id`  
   Derive from natural keys to make upserts idempotent.

5. Prune empties  
   Optionally drop empty strings and empty subdocuments before writing.

---

## Writing to MongoDB

- Use the MongoDB Spark Connector v10+.
- Upsert by `_id`. For partial updates, use an aggregation-pipeline update with `$setOnInsert` and `$set`. For full replacement, use replace-by-id.

Example upsert (aggregation pipeline form):

```json
{
  "q": { "_id": "<doc_id>" },
  "u": [
    { "$setOnInsert": { "_id": "<doc_id>" } },
    { "$set": { /* your nested fields */ } }
  ],
  "upsert": true
}
```

---

## Indexes

Start minimal; add more as query patterns stabilize.

```text
{ "member.globalId": 1, "gap.id": 1, "suspect.year": -1 }
{ "client.cliId": 1, "suspect.period": 1 }
{ "gap.status": 1, "engine.triggerEvent": 1 }
{ "flags.cpc": 1 }              # optional
{ "status.active": 1 }          # optional
{ "related.dx.flag": 1 }        # optional
```

---

## Validation checklist

- `_id` is present and unique.
- Date parse success rate is high.
- Boolean distributions look sane.
- Arrays do not contain empty strings.
- Row counts match expectations after transform.
- Spot-check a few records against source input.
- Run smoke queries: by member, by open gap, by latest engine run, by lab code.

## 5-minute demo: field + row restrictions (DB-enforced)
0) Setup sample data
```
use gaps_demo
db.members.drop()
```
Start Fresh here 
```
db.members.insertMany([
  {
    memberId: "M001",
    LOB: "Medicaid",
    name: { first: "Ava", last: "Nguyen" },
    dob: ISODate("1990-02-10"),
    ssn: "111-22-3333",
    address: { line1: "1 Main St", city: "Austin", state: "TX" }
  },
  {
    memberId: "M002",
    LOB: "Commercial",
    name: { first: "Ben", last: "Sanchez" },
    dob: ISODate("1985-07-12"),
    ssn: "222-33-4444",
    address: { line1: "2 Pine Ave", city: "Chicago", state: "IL" }
  },
  {
    memberId: "M003",
    LOB: "Medicare",
    name: { first: "Chloe", last: "Patel" },
    dob: ISODate("1979-09-30"),
    ssn: "333-44-5555",
    address: { line1: "3 Oak Rd", city: "Denver", state: "CO" }
  }
])
```

// Index so the view’s filter pushes down and stays fast
```
db.members.createIndex({ LOB: 1, memberId: 1 })
```
1) Create a secure view for offshore users
Hides Medicaid rows
Removes sensitive fields (ssn, dob, address)
```
db.runCommand({
  create: "members_offshore_v",
  viewOn: "members",
  pipeline: [
    { $match: { LOB: { $ne: "Medicaid" } } },
    { $unset: ["ssn", "dob", "address"] } // field-level redaction
  ]
})
```
2) Create roles
Internal can read the base collection (full view)
Offshore can only read the redacted view
```
db.createRole({
  role: "members_internal_reader",
  privileges: [{ resource: { db: "gaps_demo", collection: "members" }, actions: ["find"] }],
  roles: []
})

db.createRole({
  role: "members_offshore_reader",
  privileges: [{ resource: { db: "gaps_demo", collection: "members_offshore_v" }, actions: ["find"] }],
  roles: []
})
```

3) Create demo users
```
db.createUser({
  user: "aliceInternal",
  pwd: "alicePwd!",
  roles: [{ role: "members_internal_reader", db: "gaps_demo" }]
})

db.createUser({
  user: "bobOffshore",
  pwd: "bobPwd!",
  roles: [{ role: "members_offshore_reader", db: "gaps_demo" }]
})
```
4) Test (open two shells or authenticate sequentially)

As Alice (internal):
```
use gaps_demo
db.auth("aliceInternal", "alicePwd!")
db.members.find({}, { memberId: 1, LOB: 1, ssn: 1, dob: 1, address: 1, _id: 0 }).pretty()
```
/* You will see ALL LOBs and sensitive fields present */
As Bob (offshore):
```
use gaps_demo
db.auth("bobOffshore", "bobPwd!")
```
// Base collection is blocked:
```
db.members.findOne()    // => Authorization error
```
// Redacted view is allowed:
db.members_offshore_v.find({}, { memberId: 1, LOB: 1, ssn: 1, dob: 1, address: 1, _id: 0 }).pretty()
/*
Expected: Only Commercial + Medicare rows.
Fields ssn/dob/address are ABSENT (removed by the DB).
Medicaid rows are not returned at all.
*/

Optional proof: try to query for Medicaid via the view:
```
db.members_offshore_v.find({ LOB: "Medicaid" }).count()  // => 0
```
5) (Nice touch) Show it’s fast
```
db.members_offshore_v.explain("executionStats").find({ memberId: "M002" })
```
/* Look for IXSCAN on {LOB:1, memberId:1}; the $match was pushed down */
