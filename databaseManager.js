'use strict';

const AWS = require('aws-sdk');
let dynamo = new AWS.DynamoDB.DocumentClient();

const TABLE_NAME = process.env.LOCATION_LIST_DYNAMODB_TABLE;

module.exports.initializateDynamoClient = newDynamo => {
  dynamo = newDynamo;
};

module.exports.saveLocation = location => {
  const params = {
    TableName: TABLE_NAME,
    Item: location
  };

  return dynamo.put(params).promise().then(() => {
    return location.locationId;
  });
};

module.exports.getLocation = locationId => {
  const params = {
    Key: {
      locationId: locationId
    },
    TableName: TABLE_NAME
  };

  return dynamo.get(params).promise().then(result => {
    return result.Item;
  });
};

module.exports.deleteLocation = locationId => {
  const params = {
    Key: {
      locationId: locationId
    },
    TableName: TABLE_NAME
  };

  return dynamo.delete(params).promise();
};

module.exports.updateLocation = (locationId, paramsName, paramsValue) => {
  const params = {
    TableName: TABLE_NAME,
    Key: {
      locationId
    },
    ConditionExpression: 'attribute_exists(locationId)',
    UpdateExpression: 'set ' + paramsName + ' = :v',
    ExpressionAttributeValues: {
      ':v': paramsValue
    },
    ReturnValues: 'ALL_NEW'
  };

  return dynamo.update(params).promise().then(response => {
    return response.Attributes;
  });
};
