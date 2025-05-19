db.gaps.find({
  "lab.loincCode": "4548-4",
  "lab.testType": "Hemoglobin A1C",
  "lab.result": { $gt: "8.0" }
})
