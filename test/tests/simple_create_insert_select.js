'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
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
	await SQL.query('users').drop_table( true );
	await SQL.query({
		columns :
		{
			id  	: { type: 'BIGINT:UNSIGNED', increment: true },
			name  : { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : []
		}
	}, 'users' ).create_table( true );
}).timeout(100000);

it( 'Insert', async() =>
{
	insert = await SQL.query( 'users' ).insert( { name: 'john' } );
	assert.ok( insert.affected_rows, 'Insert failed ' + JSON.stringify( insert, null, '  ' ) );
}).timeout(100000);

it( 'Select', async() =>
{
	select = await SQL.query( 'users' ).where( 'id = :?', 1 ).get( '*' );
	assert.ok( select.ok && select.row && select.row.name === 'john', 'Select failed ' + JSON.stringify( select, null, '  ' ) );
}).timeout(100000);

it( 'Delete', async() =>
{
	 delete_row = await SQL.query( 'users' ).where( 'id = :?', 1 ).delete();
	assert.ok( delete_row.ok && delete_row.affected_rows, 'Deleted failed ' + JSON.stringify( delete_row, null, '  ' ) );
}).timeout(100000);

it( 'Check', async() =>
{
	select = await SQL.query( 'users' ).where( 'id = :?', 1 ).get( '*' );
	assert.ok( select.ok && !select.row, 'Select deleted row ' + JSON.stringify( select, null, '  ' ) );
}).timeout(100000);
