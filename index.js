const CollectionOne = require("./collections/collectionOne.json");
const CollectionTwo = require("./collections/collectionTwo.json");

const { MongoClient } = require("mongodb");

async function bootstrap() {
  //step 1
  const connection = await new MongoClient("mongodb://localhost:27017", {
    useUnifiedTopology: true,
  }).connect();
  const db = connection.db("dbOne");
  const collectionOne = db.collection("collectionOne");
  const collectionTwo = db.collection("collectionTwo");
  const collectionThree = db.collection("collectionThree");

  //step 2
  await collectionOne.insertMany(CollectionOne);
  await collectionTwo.insertMany(CollectionTwo);

  //step 3
  await collectionOne.updateMany({}, [
    {
      $addFields: {
        longitude: { $arrayElemAt: ["$location.ll", 0] },
        latitude: { $arrayElemAt: ["$location.ll", 1] },
      },
    },
  ]);

  //step 4
  await collectionOne
    .aggregate([
      {
        $graphLookup: {
          from: "collectionTwo",
          startWith: "$country",
          connectFromField: "country",
          connectToField: "country",
          as: "temp",
          maxDepth: 0,
        },
      },
      {
        $unwind: "$temp",
      },
      {
        $project: {
          diff: {
            $map: {
              input: "$students",
              as: "student",
              in: {
                $subtract: ["$temp.overallStudents", "$$student.number"],
              },
            },
          },
        },
      },
      {
        $unset: "temp",
      },
      { $merge: "collectionOne" },
    ])
    .toArray();

  //step 5
  await collectionTwo
    .aggregate([
      {
        $lookup: {
          from: "collectionOne",
          localField: "country",
          foreignField: "country",
          as: "tempArrayOfDocs",
        },
      },
      {
        $project: {
          count: { $size: "$tempArrayOfDocs" },
        },
      },
      {
        $unset: "tempArrayOfDocs",
      },
      { $merge: "collectionTwo" },
    ])
    .toArray();

  //step 6
  const aggregatedArray = await collectionTwo
    .aggregate([
      {
        $lookup: {
          from: "collectionOne",
          localField: "country",
          foreignField: "country",
          as: "tempArrayOfDocs",
        },
      },
      {
        $addFields: {
          longitude: [],
          latitude: [],
          allDiffs: [],
        },
      },
    ])
    .toArray();

  aggregatedArray.forEach((doc) => {
    for (temp of doc.tempArrayOfDocs) {
      doc.longitude.push(temp.longitude);
      doc.latitude.push(temp.latitude);
      doc.allDiffs.push(temp.diff);
      delete doc.country;
      delete doc.overallStudents;
      delete doc.tempArrayOfDocs;
    }
  });
  await collectionThree.insertMany(aggregatedArray);
  console.log("ready!");
  return process.exit(0);
}
bootstrap();
