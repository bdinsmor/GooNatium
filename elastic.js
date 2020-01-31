const { Client } = require('@elastic/elasticsearch');
const fs = require('fs');
require('array.prototype.flatmap').shim()
const client = new Client({
  cloud: {
    id: 'GooNatium:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQwNTM1MzAxZjFhMTY0NDYyYTk1YWI1ODYxOWY2ZmRkOCQ0ZmJmZGVkMzY2M2M0NDFhOTVjMzBmY2Q5MzkxMzA0Zg==',
  }, auth: {
       username: "elastic",
       password: "s9jBhrfgIavPvIHYSG7fUPyb"
   }
})



async function run () {
  await client.indices.create({
    index: 'locations',
    body: {
      mappings: {
        properties: {
          id: { type: 'keyword' },
          name: { type: 'keyword' },
          amenity: { type: 'keyword'},
          properties: { type: 'object'},
          location: { type: 'geo_shape'},
          doi: { type: 'date' },
        }
      }
    }
  }, { ignore: [400] })

var dataset = JSON.parse(fs.readFileSync('geodata.geojson', 'utf8'));
dataset = dataset.features;
let locations = [];
for(let i = 0; i < dataset.length; i++) {
    if(!dataset[i].properties || !dataset[i].properties.name) {
        continue;
    }
    let l = {
        properties: dataset[i].properties,
        doi: new Date(),
        name: dataset[i].properties.name,
        amenity: dataset[i].properties.amenity,
    };
    if(dataset[i].id) {
        l.id = dataset[i].id;
    } else if (dataset[i].properties && dataset[i].properties['@id']) {
        l.id = dataset[i].properties['@id'];
       
    }
    
    if(dataset[i].geometry) {
        l.location = dataset[i].geometry;
    } 
   locations.push(l);
}

  const body = locations.flatMap(doc => [{ index: { _index: 'locations', _id: doc.id }}, doc])
console.log("# of records: " + body.length);

  const { body: bulkResponse } = await client.bulk({ refresh: true, body })

  if (bulkResponse.errors) {
    const erroredDocuments = []
    // The items array has the same order of the dataset we just indexed.
    // The presence of the `error` key indicates that the operation
    // that we did for the document has failed.
    bulkResponse.items.forEach((action, i) => {
      const operation = Object.keys(action)[0]
      if (action[operation].error) {
        erroredDocuments.push({
          // If the status is 429 it means that you can retry the document,
          // otherwise it's very likely a mapping error, and you should
          // fix the document before to try it again.
          status: action[operation].status,
          error: action[operation].error,
          operation: body[i * 2],
          document: body[i * 2 + 1]
        })
      }
    })
    console.log(erroredDocuments)
  }
  
  const { body: count } = await client.count({ index: 'locations' })

  console.log("doc count: " + JSON.stringify(count,null,2));
}

run().catch(console.log);