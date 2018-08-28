'use strict';

const assert = require('assert');
const SQLError = require( '../../lib/errors.js');
const tables = require('../tables.js');
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
	await SQL.query( 'errors').drop_table( true );
	await SQL.query( 'tests').drop_table( true );
	
	await SQL.query( tables['errors'], 'errors' ).create_table( true );
	await SQL.query( tables['tests'], 'tests' ).create_table( true );
}).timeout(100000);

it( 'Errors', async() =>
{
	let cnt = 0;
	let test = await SQL.query().execute();
	assert.ok( test.error && test.error.code === 'EMPTY_QUERY' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().get();
	assert.ok( test.error && test.error.code === 'UNDEFINED_TABLE' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().get_query();
	assert.ok( test.error && test.error.code === 'UNDEFINED_TABLE' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().get_all();
	assert.ok( test.error && test.error.code === 'UNDEFINED_TABLE' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query().get_all_query();
	assert.ok( test.error && test.error.code === 'UNDEFINED_TABLE' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'tests' ).where( 'id > :?', null ).get_all( 'tests.' );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).get();
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).set( );
	assert.ok( !test.error , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).set( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'UNDEFINED_TABLE' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).update( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).delete( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).insert( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'tests' ).where('ids = :?', 'bad').update( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);
