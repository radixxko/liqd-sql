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
	await SQL('DROP TABLE IF EXISTS users').execute();
	await SQL('CREATE TABLE IF NOT EXISTS users ( id bigint unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, PRIMARY KEY (id) )').execute( );
});

it( 'Insert', async() =>
{
	insert = await SQL( 'users' ).insert( { name: 'john' } );
	assert.ok( insert.inserted_id, 'Insert failed ' + JSON.stringify( insert, null, '  ' ) );
});

it( 'Select', async() =>
{
	select = await SQL( 'users' ).where( 'id = :?', insert.inserted_id ).get( '*' );
	assert.ok( select.ok && select.row && select.row.name === 'john', 'Select failed ' + JSON.stringify( select, null, '  ' ) );
});

it( 'Delete', async() =>
{
	 delete_row = await SQL( 'users' ).where( 'id = :?', insert.inserted_id ).delete();
	assert.ok( delete_row.ok && delete_row.affected_rows, 'Deleted failed ' + JSON.stringify( delete_row, null, '  ' ) );
});

it( 'Check', async() =>
{
	select = await SQL( 'users' ).where( 'id = :?', insert.inserted_id ).get( '*' );
	assert.ok( select.ok && !select.row, 'Select deleted row ' + JSON.stringify( select, null, '  ' ) );
});
