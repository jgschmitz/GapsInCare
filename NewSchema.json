{
  "_id": "MBR12345:GAP987:2024",
  "member": {
    "globalId": "MBR12345",
    "memberId": "M123",
    "modId": "MOD1",
    "age": 67,
    "gender": "F",
    "hicNumber": "A1B2C3",
    "yearsOfService": 12
  },
  "client": {
    "cliId": "C001",
    "cliSk": "123",
    "subCliSk": "SUB9",
    "dSubCliSk": "D1",
    "clientGuid": "550e8400-e29b-41d4-a716-446655440000"
  },
  "status": {
    "active": true,
    "disenrolled": false,
    "ignore": false,
    "ignoreReason": null,
    "statusChangeInd": "N"
  },
  "engine": {
    "date": { "$date": "2024-12-01T00:00:00Z" },
    "source": "RULESET_X",
    "triggerEvent": "CLAIM",
    "gapId": "GAP987",
    "engineGapId": "ENG123",
    "batchId": "BATCH42",
    "runId": "RUN20241201"
  },
  "gap": {
    "id": "GAP987",
    "description": "Statin therapy for diabetes",
    "source": "RULE",
    "status": "OPEN",
    "statusChangeReason": "NEW",
    "reason": "No recent fill",
    "rank": 3,
    "probability": 0.74,
    "totalProbability": 0.80,
    "keyword": "statin",
    "groupId": "GRP1",
    "grouperId": "GRPR9",
    "sub": {
      "id": "SG1",
      "rank": 2,
      "reason": "A1c>8",
      "reasonInd": "HBA1C",
      "status": "OPEN",
      "frequency": "MONTHLY",
      "scoreValue": 0.6
    }
  },
  "dssCategory": {
    "code": "DSS1",
    "desc": "Diabetes",
    "factor": 1.2,
    "gapStatus": "OPEN",
    "source": "RULE"
  },
  "hcc": {
    "primary": "HCC18",
    "secondary": "HCC19",
    "group": "HCC_GRP_A",
    "desc": "Diabetes with complications",
    "factor": 0.32,
    "gapStatus": "OPEN",
    "source": "CLAIMS",
    "hierarchicalStatus": "ACTIVE"
  },
  "encounter": {
    "start": { "$date": "2024-10-10T00:00:00Z" },
    "end":   { "$date": "2024-10-12T00:00:00Z" }
  },
  "labs": {
    "hasLabs": true,
    "count": 2,
    "dos": { "$date": "2024-09-15T00:00:00Z" },
    "loinc": ["1234-5", "6789-0"],
    "types": ["A1C", "LDL"],
    "results": ["8.2", "140"]
  },
  "manifestation": {
    "flag": false,
    "count": 0,
    "dx": null,
    "icdVersion": "10"
  },
  "related": {
    "dx": { "flag": true, "count": 1, "icdVersion": "10", "codes": ["E11.9"] },
    "cpt": { "flag": false, "count": 0, "codes": [] }
  },
  "rx": {
    "flag": true,
    "count": 1,
    "ndc": "00093-7424",
    "ndcDesc": "Atorvastatin 20mg"
  },
  "provider": { "id": "PRV123" },
  "recommendation": {
    "source": "ENGINE",
    "type": "MED",
    "text": "Start or refill statin therapy",
    "value": "ATORVASTATIN 20MG"
  },
  "rules": {
    "group": "CARDIO",
    "hier": "1",
    "subHier": "A",
    "id": "RULE_1001",
    "masterId": "RM_01",
    "shortName": "STATIN_DM",
    "parent": { "id": "PRULE_10", "name": "CHRONIC_DM" },
    "version": "v5"
  },
  "coverage": {
    "modelCode": "MDL1",
    "modelVersion": "2024.1",
    "industryStdModelCode": "HEDIS",
    "metalLevel": "GOLD"
  },
  "counts": {
    "lab": 2, "rx": 1, "dc": 0, "other": 0, "relatedDx": 1, "relatedCpt": 0,
    "cpc": 0, "memberTotalDssFactor": 2.1, "memberTotalHccFactor": 0.32
  },
  "flags": {
    "cpc": false, "lab": true, "manif": false, "other": false, "relDx": true, "relCpt": false, "rx": true,
    "adultChildInd": "ADULT", "disenrollmentFlag": false
  },
  "sourceFile": {
    "name": "inbound_2024_10_15.csv",
    "date": { "$date": "2024-10-15T00:00:00Z" },
    "spectId": "SPCT1"
  },
  "suspect": { "period": "2024Q4", "year": 2024 },
  "likelihood": 0.74,
  "ttlProb": 0.80
}
