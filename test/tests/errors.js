'use strict';

const assert = require('assert');
const SQLError = require( '../../lib/errors.js');
const SQL = new (require('../../lib/sql.js'))(
{
	mysql :
	{
    host            : 'localhost',
		user            : 'root',
		password        : '',
		database		    : 'test'
	}
});

let insert, select, delete_row;

it( 'Create', async() =>
{
	await SQL.query('errors').drop_table( true );
	await SQL.query('tests').drop_table( true );

	let errors = await SQL.query( {
		columns :
		{
			id      		: { type: 'BIGINT:UNSIGNED' },
			name    		: { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : []
		}
	}, 'errors' ).create_table( true );

	let tests = await SQL.query( {
		columns :
		{
			id      		: { type: 'BIGINT:UNSIGNED', increment: true },
			name    		: { type: 'VARCHAR:255', default: 'name' },
			uid    			: { type: 'BIGINT', default: 'NULL', null: true }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : []
		}
	}, 'tests' ).create_table( true );
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
	assert.ok( test.error && test.error.code === 'INVALID_ENTRY' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL.query( 'undefined' ).delete( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL.query( 'undefined' ).insert( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL.query( 'tests' ).where('ids = :?', 'bad').update( { id: 1, name: 'John' } );
	assert.ok( test.error && test.error.code === 'EREQUEST' , 'Test error '+( ++cnt )+' failed ' + JSON.stringify( test, null, '  ' ) );

	//let error = new SQLError( ).get();

}).timeout(100000);
