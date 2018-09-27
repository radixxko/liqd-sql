'use strict';

const assert = require('assert');
const SQLError = require( '../../lib/errors.js');
const tables = require('../tables.js');
const modiefied_tables = require('../tables_m.js')

const SQL = new (require('../../lib/sql.js'))(
{
	mysql :
	{
		host     : 'localhost',
		user     : 'root',
		password : '',
		database : 'test_1'
	}
});

let insert, select, delete_row;

it( 'Create', async() =>
{
	let cnt = 0, database = 'test_1';
	//console.log('DB', await SQL.query().create_database( database, tables ));
	let test = await SQL.query().create_database( database, tables, { result_type: 'array' } );

	assert.ok( test.create && test.create.length === 23 , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);

/*
it( 'Modify', async() =>
{
	let cnt = 0;
	let test = await SQL.query().modify_database( modiefied_tables, { result_type: 'array', drop_table: true } );

	console.log( 'test', test );

	assert.ok( test.create && test.create.length === 22 , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);
*/
