db.getSiblingDB("UHG_Care").Gaps.find({
  memberId: "MBR_001",
  "gap.status": "Open"
})
