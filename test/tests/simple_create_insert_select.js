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
	await SQL('users').drop_table( true );
	await SQL({
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
});

it( 'Insert', async() =>
{
	insert = await SQL( 'users' ).insert( { name: 'john' } );
	assert.ok( insert.affected_rows, 'Insert failed ' + JSON.stringify( insert, null, '  ' ) );
});

it( 'Select', async() =>
{
	select = await SQL( 'users' ).where( 'id = :?', 1 ).get( '*' );
	assert.ok( select.ok && select.row && select.row.name === 'john', 'Select failed ' + JSON.stringify( select, null, '  ' ) );
});

it( 'Delete', async() =>
{
	 delete_row = await SQL( 'users' ).where( 'id = :?', 1 ).delete();
	assert.ok( delete_row.ok && delete_row.affected_rows, 'Deleted failed ' + JSON.stringify( delete_row, null, '  ' ) );
});

it( 'Check', async() =>
{
	select = await SQL( 'users' ).where( 'id = :?', 1 ).get( '*' );
	assert.ok( select.ok && !select.row, 'Select deleted row ' + JSON.stringify( select, null, '  ' ) );
});
