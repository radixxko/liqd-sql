'use strict';

const assert = require('assert');

it( 'Missing config', async() =>
{
	let error = null;
	try
	{
		const SQL_1 = new (require('../../lib/sql.js'))( {} );
	}
	catch(err){ error = err; };

	assert.ok( error && error.toString() === 'Error: missing config' , 'Test missing config 1 failed ' );
}).timeout(100000);

it( 'Missing database config', async() =>
{
	let error = null;
	try
	{
		const SQL_2 = new (require('../../lib/sql.js'))( { connector: config.connector } );
	}
	catch(err){ error = err; };

	assert.ok( error && error.toString() === 'Error: missing database config' , 'Test missing database config 1 failed ' );
}).timeout(100000);

it( 'Bad credentials', async() =>
{
	let error = null;

	try
	{
		const SQL_2 = new (require('../../lib/sql.js'))(
		{
			mysql :
			{
				host     : 'localhost',
				user     : 'roots',
				password : '',
				database : 'test'
			}
		});

		let test = await SQL_2.query( 'errors_list').drop_table( true );

		assert.ok( test && test.connector_error.type === 'connect' , 'Test bad credentials 1 failed ' );
	}
	catch(err){ error = err; };
}).timeout(100000);

const SQL = new (require('../../lib/sql.js'))( config );

it( 'Create', async() =>
{
	await SQL.query( 'errors_list').drop_table( true );
	await SQL.query( 'tests').drop_table( true );

	await SQL.query( config.tables['errors_list'], 'errors_list' ).create_table( true );
	await SQL.query( config.tables['tests'], 'tests' ).create_table( true );
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
	assert.ok( test.connector_error && test.connector_error.type === 'query' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).get();
	assert.ok( test.connector_error && test.connector_error.type === 'query' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).set( );
	assert.ok( !test.connector_error , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).set( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'UNDEFINED_TABLE' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).update( { id: 1, name: 'John' } );
	assert.ok( test.connector_error && test.connector_error.type === 'query' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).delete( { id: 1, name: 'John' } );
	assert.ok( test.connector_error && test.connector_error.type === 'query' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'undefined' ).insert( { id: 1, name: 'John' } );
	assert.ok( test.connector_error && test.connector_error.type === 'query' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'tests' ).where('ids = :?', 'bad').update( { id: 1, name: 'John' } );
	assert.ok( test.connector_error && test.connector_error.type === 'query' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);
