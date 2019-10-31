'use strict';

const assert = require('assert');
const modified_tables = require('../tables_m.js')
const SQL = new (require('../../lib/sql.js'))({
	[ config.connector ] : config[ config.connector ],
	connector: config.connector,
	tables: JSON.parse(JSON.stringify(config.tables))
});

let defaultRows = {
	update_users :
	[
		{ id: 1, uid: 1, name: 'John' },
		{ id: 2, uid: 2, name: 'Max' },
		{ id: 3, uid: 3, name: 'George' },
		{ id: 4, uid: 4, name: 'Janet' },
		{ id: 5, uid: 5, name: 'Kate' },
		{ id: 6, uid: 6, name: 'Tomas' }
	]
};

it( 'Schema', async() =>
{
	let cnt = 0, schema;

	schema = await SQL.database().schema();
	assert.ok( schema.ok, 'Test error '+( ++cnt )+' failed '+ '. Database schema failed' + JSON.stringify( schema, null, '  ' ) );
}).timeout(100000);

it( 'Create', async() =>
{
	let cnt = 0, database = 'test_1', test;

	test = await SQL.query().create_database( database, config.tables, { result_type: 'array' } );
	assert.ok( test.create && test.create.length === 39 , 'Test error '+( ++cnt )+' failed '+ '. Database '+ Object.keys( config.tables ).length + 1 +' length. Created length '+ test.create.length + '. ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().create_database( database, [ config.tables ], { result_type: 'array', default_rows: defaultRows } );
	assert.ok( !test.ok , 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().create_database( [ 'name' ], config.tables, { result_type: 'array', default_rows: defaultRows } );
	assert.ok( !test.ok , 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().create_database( database, config.tables, { result_type: 'arrays' } );
	assert.ok( test.ok && typeof test.create === 'string', 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().create_database( database, config.tables, { result_type: 'array', drop_table: true, default_rows: [ { table: '' } ] } );
	assert.ok( test.ok , 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().create_database( database, config.tables, { result_type: 'array', default_rows: defaultRows } );
	assert.ok( test.create && test.create.length === 39, 'Test error '+( ++cnt )+' failed '+ '. Database '+ Object.keys( config.tables ).length + 1 +' length. Created length '+ test.create.length + '. ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().create_database( database, config.tables, { drop_table: true } );
	assert.ok( test.ok && test.create && typeof test.create === 'string' , 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.database( database ).create_query( config.tables, { result_type: 'array' } );
	assert.ok( test.ok && test.create && Array.isArray( test.create ), 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.database( database ).create_query( config.tables );
	assert.ok( test.ok && test.create && typeof test.create === 'string', 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.database( database ).create( config.tables );
	assert.ok( test.ok , 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	//test = await SQL.query().create_database( database, config.tables, { result_type: 'execute', drop_table: true } );
	//assert.ok( test.ok && test.create && typeof test.create === 'string' , 'Test create_database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);

it( 'Drop', async() =>
{
	let cnt = 0, database = 'test_1', test;

	test = await SQL.database( database ).drop_query();
	assert.ok( test.ok , 'Test drop database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.database( database ).drop();
	assert.ok( test.ok , 'Test drop database '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );
}).timeout(100000);


it( 'Modify', async() =>
{
	let test, cnt = 0;
	test = await SQL.query().modify_database( modified_tables, { result_type: 'array', drop_table: true } );
	assert.ok( test , 'Test modify_table '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().modify_database( {}, { result_type: 'array', drop_table: true, default_rows: [ { table: '' } ] } );
	assert.ok( test && !test.ok , 'Test modify_table '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().modify_database( modified_tables, { result_type: 'array', drop_table: true, default_rows: [ { table: '' } ] } );
	assert.ok( test && !test.ok , 'Test modify_table '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().modify_database( modified_tables, { result_type: 'arrays', default_rows: defaultRows } );
	assert.ok( test && !test.ok , 'Test modify_table '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().modify_database( modified_tables, { drop_table: true } );
	assert.ok( test && test.ok , 'Test modify_table '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().modify_database( modified_tables, { result_type: 'array', drop_table: true, default_rows: defaultRows } );
	assert.ok( test && test.ok , 'Test modify_table '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );
}).timeout(100000);
