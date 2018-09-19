'use strict';

const assert = require('assert');
const SQLError = require( '../../lib/errors.js');
const tables = require('../tables.js')

const SQL = new (require('../../lib/sql.js'))(
{
	mysql :
	{
		host     : 'localhost',
		user     : 'root',
		password : '',
		database : 'test'
	}
});

let insert, select, delete_row;

it( 'Create', async() =>
{
	let cnt = 0, database = 'test_1';
	let test = await SQL.query().create_database( database, tables, { result_type: 'array' } );

	assert.ok( test.create && test.create.length === 21 , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);
