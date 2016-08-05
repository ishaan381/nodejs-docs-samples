// Copyright 2016, Google, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * [START complete]
 * Command-line application to perform an synchronous query in BigQuery.
 *
 * This sample is used on this page:
 *
 *   https://cloud.google.com/bigquery/querying-data#syncqueries
 */

'use strict';

// [START auth]
// By default, gcloud will authenticate using the service account file specified
// by the GOOGLE_APPLICATION_CREDENTIALS environment variable and use the
// project specified by the GCLOUD_PROJECT environment variable. See
// https://googlecloudplatform.github.io/gcloud-node/#/docs/guides/authentication
var gcloud = require('gcloud');

// Get a reference to the bigquery component
var bigquery = gcloud.bigquery();
// [END auth]

// [START query]
/**
 * Run an example synchronous query.
 * @param {string} query The BigQuery query to run, as a string.
 * @param {Function} callback Callback function.
 * @param {Object} queryOpts (Optional) Additional query options.
 *        See https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
 *        for a comprehensive list
 */
function syncQuery (query, callback, queryOpts) {
  if (!query || typeof query !== 'string') {
    throw new Error('query cannot be null');
  }
  if (!queryOpts) {
    queryOpts = {};
  }
  queryOpts.query = query;

  var allRows = [];
  var paginator = function (err, rows, nextQuery) {
    if (err) {
      callback(err);
    }
    allRows = allRows.concat(rows);
    if (nextQuery) {
      bigquery.query(nextQuery, paginator);
    } else {
      callback(null, allRows);
    }
  };

  bigquery.query(queryOpts, paginator);
}
// [END query]
// [END complete]

// The command-line program
var program = {
  // Print usage instructions
  printUsage: function () {
    console.log('Usage: node sync_query.js QUERY');
  },

  // Run the sample
  main: function (args, cb) {
    if (args.length === 1 && !(args[0] === '-h' || args[0] === '--help')) {
      syncQuery(args[0], cb);
    } else {
      program.printUsage();
    }
  },

  // Exports
  syncQuery: syncQuery
};

if (module === require.main) {
  program.main(process.argv.slice(2), console.log);
}
module.exports = program;
