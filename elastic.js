const { Client } = require("@elastic/elasticsearch");
const fs = require("fs");
const request = require("request-promise");
require("array.prototype.flatmap").shim();
const client = new Client({
  cloud: {
    id: "GooNatium:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQwNTM1MzAxZjFhMTY0NDYyYTk1YWI1ODYxOWY2ZmRkOCQ0ZmJmZGVkMzY2M2M0NDFhOTVjMzBmY2Q5MzkxMzA0Zg=="
  },
  auth: {
    username: "elastic",
    password: "s9jBhrfgIavPvIHYSG7fUPyb"
  }
});

async function getGoogleDetails(name, location) {
  const coords = location.coordinates;
  const key = "CHECK LOCAL";
  const fieldsToReturn = [
    "formatted_address",
    "name",
    "geometry",
    "permanently_closed",
    "place_id",
    "photos",
    "types",
    "plus_code"
  ];
  // price_level, rating, user_ratings_total, opening_hours cost more money to get back...

  // using this search will only return hits where the OSM name is indexed within the Google data somewhere
  const locationSearch =
    "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=" +
    encodeURIComponent(name) +
    "&inputtype=textquery&fields=" +
    fieldsToReturn.join(",") +
    "&locationbias=circle:100@" +
    coords[1] +
    "," +
    coords[0] +
    "&key=" +
    key;
  
  /**
   * this URL is if you want to do a distance search based on the coordinates
   * const url =
    "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=" +
    coords[1] +
    "," +
    coords[0] +
    "&rankby=distance&types=restaurant&key=" +
    apiKey;
**/
  
  try {
    const res = await request(locationSearch);
    if (res) {
      const body = JSON.parse(res);
      if (body && body.candidates && body.candidates.length > 0) {
        const results = body.candidates[0];
        return results;
      } else {
        return {};
      }
    }
  } catch (e) {
    console.log("caught error: " + e);
  }
}

async function run() {
  await client.indices.delete({
    index: "locations"
  });
  console.log("deleted locations index!");
  await client.indices.create(
    {
      index: "locations",
      body: {
        mappings: {
          properties: {
            id: { type: "keyword" },
            name: { type: "keyword" },
            amenity: { type: "keyword" },
            properties: { type: "object" },
            location: { type: "geo_shape" },
            doi: { type: "date" }
          }
        }
      }
    },
    { ignore: [400] }
  );
console.log("created locations index!");
  let dataset = JSON.parse(fs.readFileSync("berlin2.geojson", "utf8"));
  dataset = dataset.features;
  const locations = [];
  const max = 25;
  if (dataset.length > max) {
    dataset = dataset.slice(0, max);
  }
  for (let i = 0; i < dataset.length; i++) {
    if (!dataset[i].properties || !dataset[i].properties.name) {
      continue;
    }
    const l = {
      properties: dataset[i].properties,
      doi: new Date(),
      name: dataset[i].properties.name,
      amenity: dataset[i].properties.amenity
    };
    if (dataset[i].id) {
      l.id = dataset[i].id;
    } else if (dataset[i].properties && dataset[i].properties["@id"]) {
      l.id = dataset[i].properties["@id"];
    }

    if (dataset[i].geometry) {
      l.location = dataset[i].geometry;
      l.googleDetails = await getGoogleDetails(l.name, l.location);
    }

      console.log("finished " + i);
    
    locations.push(l);
  }
  
  console.log("# of records to index: " + location.length);
  const body = locations.flatMap(doc => [{ index: { _index: "locations", _id: doc.id } }, doc]);
  const { body: bulkResponse } = await client.bulk({ refresh: true, body });

  if (bulkResponse.errors) {
    const erroredDocuments = [];
    // The items array has the same order of the dataset we just indexed.
    // The presence of the `error` key indicates that the operation
    // that we did for the document has failed.
    bulkResponse.items.forEach((action, i) => {
      const operation = Object.keys(action)[0];
      if (action[operation].error) {
        erroredDocuments.push({
          // If the status is 429 it means that you can retry the document,
          // otherwise it's very likely a mapping error, and you should
          // fix the document before to try it again.
          status: action[operation].status,
          error: action[operation].error,
          operation: body[i * 2],
          document: body[i * 2 + 1]
        });
      }
    });
    console.log(erroredDocuments);
  }

  const { body: count } = await client.count({ index: "locations" });
 console.log("doc count: " + JSON.stringify(count, null, 2));
}

run().catch(console.log);
