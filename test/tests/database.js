'use strict';

const assert = require('assert');
const modiefied_tables = require('../tables_m.js')
const SQL = new (require('../../lib/sql.js'))( config );

it( 'Create', async() =>
{
	let cnt = 0, database = 'test_1';

	let test = await SQL.query().create_database( database, config.tables, { result_type: 'array' } );
	assert.ok( test.create && test.create.length === 24 , 'Test error '+( ++cnt )+' failed '+ '. Length '+ test.create.length + '. ' + JSON.stringify( test, null, '  ' ) );

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
