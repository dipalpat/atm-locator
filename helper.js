const AWS = require('aws-sdk');
const _ = require('lodash');
const TOPIC_NAME = process.env.STREAM_LOCATION_TOPIC

const docClient = new AWS.DynamoDB.DocumentClient();

const googleApiClient = require('@google/maps').createClient({
    key: process.env.GOOGLE_GEOCODE_API_KEY
});

const algoliasearch = require('algoliasearch');
const algoliaClient = algoliasearch(process.env.ALGOLIA_APP_ID, process.env.ALGOLIA_ADMIN_API_KEY);
const algoliaIndex = algoliaClient.initIndex(process.env.ALGOLIA_INDEX_NAME);

const SlackWebHook = require('slack-webhook');
const slack = new SlackWebHook('https://hooks.slack.com/services/' + process.env.SLACK_WEBHOOK_ID);

const sns = new AWS.SNS();

exports.getData = () => {

    const params = {
        TableName: process.env.LOCATION_LIST_DYNAMODB_TABLE
    };

    return new Promise((resolve, reject) => {

        docClient.scan(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data)
            }

        });


    });




};

exports.findGeoCode = addressText => {

    return new Promise((resolve, reject) => {

        googleApiClient.geocode({
            address: addressText
        }, (err, response) => {
            if (err) {
                reject(err);
            }
            if (response.json.results.length > 0) {
                const geometry = response.json.results[0].geometry;
                resolve(geometry.location);
            } else {
                resolve(null);
            }
        });

    });

};

exports.startStateMachine = location => {

    const params = {
        stateMachineArn: process.env.statemachine_arn,
        input: JSON.stringify(location)
    };

    const stepfunctions = new AWS.StepFunctions();
    stepfunctions.startExecution(params, (err, data) => {

        if (err) {
            console.log(err);
        } else {
            console.log('State Machine started successfully');
            console.log(data);
        }

    });
};

exports.pushToAlgolia = location => {
    return algoliaIndex.addObject(location);
};

exports.sentToSlack = message => {
    slack.send(message)
        .then(data => {
            console.log(data);
        })
        .catch(err => {
            console.log(err);
        })
};

exports.removeFromAlgolia = locationId => {
    return algoliaIndex.deleteObject(locationId);
};

exports.updateAlgolia = location => {
    algoliaIndex.partialUpdateObject(location, (err, res) => {
        if (err) {
            console.log(err);
        } else {
            console.log(res);
            this.sentToSlack(`${location.locationId} updated in algolia`)
        }
    })
};

exports.searchAlgolia = geocodes => {
    return algoliaIndex.search({
        aroundLatLng: `${geocodes.lat}, ${geocodes.lng}`,
        aroundRadius: 7000

    });
};


exports.sendToLocationListSNS = event => {

    var params = {
        Subject: 'Location List Updated',
        Message: JSON.stringify(event),
        TopicArn: process.env.topicARN
    };
    sns.publish(params, function (err, data) {
        if (err) {
            console.error("Unable to send message. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Results from sending message: ", JSON.stringify(data, null, 2));
        }
    });

};

