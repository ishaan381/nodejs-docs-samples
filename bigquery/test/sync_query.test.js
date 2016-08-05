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

'use strict';

var example = require('../sync_query');
sinon.spy(example, 'printUsage');

describe('bigquery:sync_query', function () {
  describe('main', function () {
    it('should show usage if no arguments exist', function () {
      example.main([],
        function (err) {
          assert.ifError(err);
          assert(example.printUsage.called);
        }
      );
    });

    it('should show usage if first argument is -h', function () {
      example.main(['-h'],
        function (err) {
          assert.ifError(err);
          assert(example.printUsage.called);
        }
      );
    });

    it('should show usage if first argument is --help', function () {
      example.main(['--help'],
        function (err) {
          assert.ifError(err);
          assert(example.printUsage.called);
        }
      );
    });
  });

  describe('printUsage', function () {
    it('should print usage', function () {
      example.printUsage();
      assert(console.log.calledWith('Usage: node sync_query.js QUERY'));
    });
  });

  describe('sync_query', function () {
    it('should fetch data given a query', function () {
      example.syncQuery(
        'SELECT * FROM publicdata:samples.natality LIMIT 5;',
        function (err, data) {
          assert.ifError(err);
          assert.notNull(data);
          assert(Array.isArray(data));
          assert(data.length === 5);
        },
        {
          maxResults: 10
        }
      );
    });

    it('should paginate', function () {
      example.syncQuery(
        'SELECT * FROM publicdata:samples.natality LIMIT 50;',
        function (err, data) {
          assert.ifError(err);
          assert.notNull(data);
          assert(Array.isArray(data));
          assert(data.length === 50);
        },
        {
          maxResults: 10
        }
      );
    });

    it('should override queries in queryOpts', function () {
      example.syncQuery(
        'SELECT * FROM publicdata:samples.natality LIMIT 10;',
        function (err, data) {
          assert.ifError(err);
          assert.notNull(data);
          assert(Array.isArray(data));
          assert(data.length === 10);
        },
        {
          query: 'SELECT * FROM publicdata:samples.natality LIMIT 5;',
          maxResults: 10
        }
      );
    });
  });
});
