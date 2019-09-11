'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
const SQL = new (require('../../lib/sql.js'))( config );
let tables = require('./../all_tables.js');

it( 'Dual', async() =>
{
	let dual, assert_check, cnt = 0;
	let data1 = [ { id : 5, name: 'Kate' }, { id: 78, name: 'Iron' }, { id: 123, name: 'Cooper' } ];

	dual = await SQL.query( data1, 'alias' ).get_all();

	assert_check = await config.compare_array( dual.rows, [ { id : 5, name: 'Kate' }, { id: 78, name: 'Iron' }, { id: 123, name: 'Cooper' } ], ++cnt, dual, 'dual' );

}).timeout(100000);
