db.getSiblingDB("UHG_Care").Gaps.find({
  "lab.loincCode": "4548-4",
  "lab.result": { $gt: "8" }
})
