const pipeline = [
  { $match: { operationType: "insert" } }
];

const changeStream = db.getSiblingDB("UHG_Care").Gaps.watch(pipeline);

changeStream.on("change", (next) => {
  console.log("New gap detected:", next.fullDocument);
  // TODO: trigger webhook or notification logic here
});
