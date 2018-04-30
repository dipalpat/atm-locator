const helper = require('./helper');
const _ = require('lodash');
const databaseManager = require('./databaseManager');
const uuidv1 = require('uuid/v1');
const AWS = require('aws-sdk');
let dynamo = new AWS.DynamoDB.DocumentClient();


module.exports.firstRun = (event, context, callback) => {

  helper.getData()
    .then(results => {
      _.forEach(results.Locations, location => {
        console.log('Starting state-machine for this location - ' + location.locationId);
        helper.startStateMachine(location);
      });
    })
    .catch(err => {
      console.log(err);
      callback(err);
    })
};


module.exports.findGeoCode = (event, context, callback) => {

  const location = event;
  const addressText = `${location.line1}, ${location.city}, ${location.zipCode}`;
  location.searchable = false;

  helper.findGeoCode(addressText)
    .then(geoCodes => {
      if (geoCodes) {
        location._geoloc = {
          lat: geoCodes.lat,
          lng: geoCodes.lng
        };
        location.searchable = true;
      }
      callback(null, location);
    })
    .catch(err => {
      callback(err);
    })

};

module.exports.pushToAlgolia = (event, context, callback) => {

  const location = event;
  location.objectID = location.locationId;
  helper.pushToAlgolia(location)
    .then(results => {
      const message = `${location.locationId} pushed to algolia successfully`;
      helper.sentToSlack(message);
      callback(null, message);
    })
    .catch(err => {
      callback(err);
    });

};

module.exports.locationFailed = (event, context, callback) => {
  const message = `location ${event.locationId} not pushed to Algolia`;
  helper.sentToSlack(message);
  callback(null, message);

};

module.exports.processUpdates = (event, context, callback) => {

  console.log(`event:\n${JSON.stringify(event, null, 2)}`)
  helper.sendToLocationListSNS(event);
};

module.exports.findLocations = (event, context, callback) => {
  const address = event.queryStringParameters.address;
  helper.findGeoCode(address)
    .then(geocodes => {
      if (geocodes) {
        helper.searchAlgolia(geocodes)
          .then(results => {

            const response = {
              statusCode: 200,
              body: JSON.stringify(results)
            };

            callback(null, response);
          })
          .catch(err => {
            const response = {
              statusCode: 500,
              body: 'Internal server error ' + err
            };
            callback(null, response);
          })
      } else {
        const response = {
          statusCode: 400,
          body: 'Invalid address ' + address
        };
        callback(null, response);
      }
    })
};

module.exports.dbUpdateConsumer = (event, context, callback) => {

  console.log(JSON.stringify(event));
  event.Records.forEach(record => {

    const eventData = JSON.parse(record.Sns.Message)
    eventData.Records.forEach(record => {
      if (record.eventName === 'INSERT') {
        const data = record.dynamodb.NewImage;
        const location = {
          locationId: data.locationId.S,
          line1: data.line1.S,
          line2: data.line2.S,
          city: data.city.S,
          state: data.state.S,
          country: data.country.S,
          name: data.name.S,
          zipCode: data.zipCode.S
        };
        helper.startStateMachine(location);

      } else if (record.eventName === 'MODIFY') {
        const data = record.dynamodb.NewImage;
        const oldData = record.dynamodb.OldImage;

        const location = {
          locationId: data.locationId.S,
          line1: data.line1.S,
          line2: data.line2.S,
          city: data.city.S,
          state: data.state.S,
          country: data.country.S,
          zipCode: data.zipCode.S,
          name: data.name.S,
          objectID: data.locationId.S
        }


        helper.updateAlgolia(location);

      } else if (record.eventName === 'REMOVE') {
        const data = record.dynamodb.OldImage;
        const locationId = data.locationId.S;

        helper.removeFromAlgolia(locationId)
          .then(() => {
            helper.sentToSlack(`${locationId} was removed from algolia`);
          })
          .catch(err => {
            console.log(err);
            helper.sentToSlack(err);
          })


      }
    });

  });

};

function createResponse(statusCode, message) {
  return {
    statusCode: statusCode,
    body: JSON.stringify(message)
  };
}

module.exports.saveLocation = (event, context, callback) => {
  const location = JSON.parse(event.body);
  console.log(location);
  location.locationId = uuidv1();

  databaseManager.saveLocation(location).then(response => {
    console.log(response);
    callback(null, createResponse(200, response));
  });
};

module.exports.getLocation = (event, context, callback) => {
  const locationId = event.pathParameters.locationId;

  databaseManager.getLocation(locationId).then(response => {
    console.log(response);
    callback(null, createResponse(200, response));
  });
};

module.exports.deleteLocation = (event, context, callback) => {
  const locationId = event.pathParameters.locationId;

  databaseManager.deleteLocation(locationId).then(response => {
    callback(null, createResponse(200, 'Location was deleted'));
  });
};

// module.exports.updateLocation = (event, context, callback) => {
//   const locationId = event.pathParameters.locationId;

//   const body = JSON.parse(event.body);
//   const paramName = body.paramName;
//   const paramValue = body.paramValue;

//   databaseManager.updateLocation(locationId, paramName, paramValue).then(response => {
//     console.log(response);
//     callback(null, createResponse(200, response));
//   });
// };

// exports.updateLocation = function(event, context, callback) {
//   //console.log(JSON.stringify(event));
//   const payload = _.forOwn(event.body, (memo, value, key) => {
//     memo.ExpressionAttributeNames[`#${key}`] = key
//     memo.ExpressionAttributeValues[`:${key}`] = value
//     memo.UpdateExpression.push(`#${key} = :${key}`)
//     return memo
//   }, {
//     TableName: process.env.LOCATION_LIST_DYNAMODB_TABLE,
//     Key: { locationId: event.pathParameters.locationId },
//     UpdateExpression: [],
//     ExpressionAttributeNames: {},
//     ExpressionAttributeValues: {}
//   })
//   //payload.UpdateExpression = 'SET ' + payload.UpdateExpression.join(', ')
//   console.log(payload);
//   //dynamo.update(payload, context.done)
// }

exports.updateLocation = (event, context, callback) => {
  let a = JSON.parse(event.body);
  const payload = _.reduce(a, (memo, value, key) => {
    console.log('[' + key + '] = ' + value); 
    
    memo.ExpressionAttributeNames[`#${key}`] = key
    memo.ExpressionAttributeValues[`:${key}`] = value
    memo.UpdateExpression.push(`#${key} = :${key}`)
     return memo
  }, {
    TableName: process.env.LOCATION_LIST_DYNAMODB_TABLE,
    Key: { locationId: event.pathParameters.locationId },
    UpdateExpression: [],
    ExpressionAttributeNames: {},
    ExpressionAttributeValues: {},
    ReturnValues: 'ALL_NEW'
  })
  payload.UpdateExpression = 'SET ' + payload.UpdateExpression.join(', ')
  console.log(payload);
  return dynamo.update(payload).promise().then(response => {
    callback(null, createResponse(200, response));
  });
}

