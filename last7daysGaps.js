db.gaps.find({
  "engine.executionDate": {
    $gte: ISODate("2025-05-12T00:00:00Z")
  }
})
