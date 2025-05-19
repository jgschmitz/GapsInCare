db.getSiblingDB("UHG_Care").Gaps.find({
  "engine.executionDate": { $gte: ISODate("2025-05-15T00:00:00Z")
