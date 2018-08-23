/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload and metadata.
 * @param {!Function} callback Callback function to signal completion.
 */

// TEST ENV
// Payload example: [{"timestamp":1533564208,"device_id":"1B2DEB","temperature":19.1},{"timestamp":1533563008,"device_id":"1B2DEB","temperature":23.5},{"timestamp":1533561808,"device_id":"1B2DEB","temperature":23.5}]
// JSON:  W3sidGltZXN0YW1wIjoxNTMzNTY0MjA4LCJkZXZpY2VfaWQiOiIxQjJERUIiLCJ0ZW1wZXJhdHVyZSI6MTkuMX0seyJ0aW1lc3RhbXAiOjE1MzM1NjMwMDgsImRldmljZV9pZCI6IjFCMkRFQiIsInRlbXBlcmF0dXJlIjoyMy41fSx7InRpbWVzdGFtcCI6MTUzMzU2MTgwOCwiZGV2aWNlX2lkIjoiMUIyREVCIiwidGVtcGVyYXR1cmUiOjIzLjV9XQ==
// functions call timeseriesbigquery --data='{ "data":"W3sidGltZXN0YW1wIjoxNTMzNTY0MjA4LCJkZXZpY2VfaWQiOiIxQjJERUIiLCJ0ZW1wZXJhdHVyZSI6MTkuMX0seyJ0aW1lc3RhbXAiOjE1MzM1NjMwMDgsImRldmljZV9pZCI6IjFCMkRFQiIsInRlbXBlcmF0dXJlIjoyMy41fSx7InRpbWVzdGFtcCI6MTUzMzU2MTgwOCwiZGV2aWNlX2lkIjoiMUIyREVCIiwidGVtcGVyYXR1cmUiOjIzLjV9XQ==","attributes": {"device":"1B2DEB","time":"1533564208","deviceType":"nke_model"}}'

// Dependencies call
const Buffer = require('safe-buffer').Buffer;
const BigQuery = require('@google-cloud/bigquery');

const projectId = 'iot-test-212508';
const keyFilename = './iot-test-212508-b0b52a0361c5.json';
const datasetId = 'nke_model'; //Could be retrieved from an attribute or a DB to have a dataset per customer
const tableId = 'temperature'; //Could be setup as the temperature_<device ID>

// // TODO:
// + Screen dataset based on the device type, create a table based on the attributes table id,
// create a data schema based on the JSON array, fill in the dataset
// const schema = "Name:string, Age:integer, Weight:float, IsMagic:boolean";
// timeseries_big-query

exports.timeseriesbigquery = (event, callback) => {
  // Creates a BigQuery client
  const bigquery = new BigQuery({
    projectId: projectId,
    keyFilename: keyFilename
  });

  // Gets the data from the pubsub broker and prepares it to be Inserted
  const pubsubMessage = event.data;

  // Extract payload
  var payload = Buffer.from(pubsubMessage.data, 'base64').toString();
  try {
    var payload2 = JSON.parse(payload);
  } catch (err) {
    console.log('Error', err);
  }

  // Save data in big query
  const rows = payload2;
  // Rows must be compatible with the data schema specified above
  // Example of rows: [{"timestamp":1533564208,"device_id":"1B2DEB","temperature":19.1},{"timestamp":1533563008,"device_id":"1B2DEB","temperature":23.5},{"timestamp":1533561808,"device_id":"1B2DEB","temperature":23.5}]
  bigquery
    .dataset(datasetId)
    .table(tableId)
    .insert(rows)
    .then(() => {
      console.log(`Inserted row`);
    })
    .catch(err => {
      if (err && err.name === 'PartialFailureError') {
        if (err.errors && err.errors.length > 0) {
          console.log('Insert errors:');
          err.errors.forEach(err => console.error(err));
        }
      } else {
        console.error('ERROR:', err);
      }
    });
  callback();
};

// If necessary for JSON characters issues
function escapeSpecialChars(jsonString) {
  return jsonString
    .replace(/\\n/g, '\\n')
    .replace(/\\'/g, "\\'")
    .replace(/\\"/g, '\\"')
    .replace(/\\&/g, '\\&')
    .replace(/\\r/g, '\\r')
    .replace(/\\t/g, '\\t')
    .replace(/\\b/g, '\\b')
    .replace(/\\f/g, '\\f')
    .replace(/[\u0000-\u0019]+/g, '')
    .replace(/[\u0028-\u0029]+/g, '');
}
