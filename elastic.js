const { Client } = require("@elastic/elasticsearch");
const fs = require("fs");
const request = require("request-promise");

require("dotenv").config();

require("array.prototype.flatmap").shim();
const client = new Client({
  cloud: {
    //id: "GooNatium:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQwNTM1MzAxZjFhMTY0NDYyYTk1YWI1ODYxOWY2ZmRkOCQ0ZmJmZGVkMzY2M2M0NDFhOTVjMzBmY2Q5MzkxMzA0Zg=="
    id: "placesearch:ZXUtY2VudHJhbC0xLmF3cy5jbG91ZC5lcy5pbyQ0Njc1MDRkNGY0OTY0MmNkOGY3ZDk5MmRjMDJjMDdhMSQ4Y2YxMTQ4ZTdkNjg0MDA2YmRkYmNjN2E4MmY2ZTkzOA=="
  },
  auth: {
    username: "elastic",
    // password: "s9jBhrfgIavPvIHYSG7fUPyb"
    password: process.env.ELASTICPW
  }
});

async function getGoogleDetails(name, location) {
  const fieldsToReturn = ["price_level", "rating", "user_ratings_total", "opening_hours","formatted_address", "name", "geometry", "permanently_closed", "place_id", "photos", "types", "plus_code"];
  // price_level, rating, user_ratings_total, opening_hours cost more money to get back...

  // using this search will only return hits where the OSM name is indexed within the Google data somewhere

  const apiKey = process.env.GOOGLEAPIKEY;

  const locationSearch =
    "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=" +
    encodeURIComponent(name) +
    "&inputtype=textquery&fields=" +
    fieldsToReturn.join(",") +
    "&locationbias=circle:100@" +
    location.lat +
    "," +
   location.lon +
    "&key=" +
    apiKey;

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
            tags: { type: "object" },
            googleDetails: { type: "object"},
            location: { type: "geo_shape" },
            doi: { type: "date" }
          }
        }
      }
    },
    { ignore: [400] }
  );
  console.log("created locations index!");
  let gooData = null;
  const overpassUrl = "http://overpass-api.de/api/interpreter";
  try {
    if (fs.existsSync("goonatim.json")) {
      gooData = JSON.parse(fs.readFileSync("goonatim.json", "utf8"));
    }
  } catch (err) {
    console.error(err);
  }
  if (!gooData) {
                  //query overpass interpreter api to get OSM results for bounding box - post body payload has to be URI encoded
                  gooData = [];
                  const postBody =
                    "data=%0A%5Bout%3Ajson%5D%5Btimeout%3A1000%5D%5Bbbox%3A52.26311463698559%2C13.18702697753906%2C52.74959372674114%2C13.663558959960936%5D%3B%0A(+node%5B%22amenity%22~%22restaurant%7Cbar%7Ccafe%22%5D%3B%0A++way%5B%22amenity%22~%22restaurant%7Cbar%7Ccafe%22%5D%3B%0A++relation%5B%22amenity%22~%22restaurant%7Cbar%7Ccafe%22%5D%3B+)%3B%0Aout+center%3B%0A";
                  // data=[out:json][timeout:1000][bbox:52.26311463698559,13.18702697753906,52.74959372674114,13.663558959960936];(+node["amenity"~"restaurant|bar|cafe"];++way["amenity"~"restaurant|bar|cafe"];++relation["amenity"~"restaurant|bar|cafe"];+);out+center;
                  let overpassRes = await request({ url: overpassUrl, form: postBody });
                  if (overpassRes) {
                    overpassRes = JSON.parse(overpassRes);
                  }
                  if (overpassRes && overpassRes.elements) {
                    dataset = overpassRes.elements;
                    console.log("overpass returned: " + dataset.length + " results");
                  }

                  // didnt find enriched google data locally, will reach out to google places API and grab it again
                  // let dataset = JSON.parse(fs.readFileSync("berlin2.geojson", "utf8"));

                  const max = 25;
                  if (dataset.length > max) {
                    dataset = dataset.slice(0, max);
                  }
                  for (let i = 0; i < dataset.length; i++) {
                    if (!dataset[i].tags || !dataset[i].tags.name) {
                      continue;
                    }
                    const l = {
                      tags: dataset[i].tags,
                      doi: new Date(),
                      name: dataset[i].tags.name,
                      amenity: dataset[i].tags.amenity
                    };
                    if (dataset[i].id) {
                      l.id = dataset[i].id;
                    } else if (dataset[i].tags && dataset[i].tags["@id"]) {
                      l.id = dataset[i].tags["@id"];
                    }

                    if (dataset[i].lat && dataset[i].lon) {
                      l.location = {
                        lat: dataset[i].lat,
                        lon: dataset[i].lon
                      };

                      l.googleDetails = await getGoogleDetails(l.name, l.location);
                      console.log("google details: " + JSON.stringify(l.googleDetails, null, 2));
                    }
                    console.log("finished: " + (i + 1));
                    gooData.push(l);
                  }
                  fs.writeFileSync("goonatim.json", JSON.stringify(gooData));
                }


  console.log("# of records to index: " + gooData.length);
  
  
  const body = gooData.flatMap(doc => [{ index: { _index: "locations", _id: doc.id } }, doc]);
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
