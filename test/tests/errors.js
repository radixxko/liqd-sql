'use strict';

const assert = require('assert');
const SQL = require('../../lib/sql.js')(
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
  await SQL('CREATE TABLE IF NOT EXISTS errors ( id bigint unsigned NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
  await SQL('CREATE TABLE IF NOT EXISTS tests ( id bigint unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL default \'name\', PRIMARY KEY (id) )' ).execute();
});

it( 'Errors', async() =>
{
  let cnt = 0;
	let test = await SQL().execute();
	assert.ok( test.error === 'empty_query' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL().get();
	assert.ok( test.error === 'undefined_table' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL().get_query();
	assert.ok( test.error === 'undefined_table' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL().get_all();
	assert.ok( test.error === 'undefined_table' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL().get_all_query();
	assert.ok( test.error === 'undefined_table' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'undefined' ).get();
	assert.ok( test.error.code === 'ER_NO_SUCH_TABLE' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'undefined' ).set( );
	assert.ok( test.error === null , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'undefined' ).set( { id: 1, name: 'John' } );
	assert.ok( test.error.code === 'ER_NO_SUCH_TABLE' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'undefined' ).update( { id: 1, name: 'John' } );
	assert.ok( test.error.code === 'ER_NO_SUCH_TABLE' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'undefined' ).delete( { id: 1, name: 'John' } );
	assert.ok( test.error.code === 'ER_NO_SUCH_TABLE' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'undefined' ).insert( { id: 1, name: 'John' } );
	assert.ok( test.error.code === 'ER_NO_SUCH_TABLE' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'tests' ).where('ids = :?', 'bad').update( { id: 1, name: 'John' } );
	assert.ok( test.error.code === 'ER_BAD_FIELD_ERROR' , 'Test error '+( cnt++ )+' failed ' + JSON.stringify( test, null, '  ' ) );

});
