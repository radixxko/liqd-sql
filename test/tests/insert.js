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
	await SQL('insert_users').drop_table( true );
	await SQL('insert_users_2').drop_table( true );

	await SQL({
		columns :
		{
			id      		: { type: 'BIGINT:UNSIGNED', increment: true },
			name    		: { type: 'VARCHAR:255' },
			description : { type: 'TEXT', null: true },
			created 		: { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			surname    	: { type: 'VARCHAR:55', null: true }
		},
		indexes : {
			primary : 'id',
			unique  : ['name'],
			index   : 'surname'
		}
	}, 'insert_users' ).create_table( true );

	await SQL({
		columns :
		{
			id      		: { type: 'BIGINT:UNSIGNED' },
			name    		: { type: 'VARCHAR:255' },
			surname 		: { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id,name',
			unique  : []
		}
	}, 'insert_users_2' ).create_table( true );
});

it( 'Insert', async() =>
{
  let cnt = 0;
  let insert = await SQL( ).insert( [ { name: 'John' }, { name: 'Max' } ] );
  assert.ok( insert.error && insert.error.code === 'UNDEFINED_TABLE', 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL( 'insert_users' ).insert( );
  assert.ok( insert.ok && insert.affected_rows === 0, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL( 'insert_users' ).insert( [ { name: 'John' }, { name: 'Max' } ] );
  assert.ok( insert.ok && insert.affected_rows === 2, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

  insert = await SQL( 'insert_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' } ], 'ignore' );
  assert.ok( insert.ok && insert.affected_rows === 2 , 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL( 'insert_users_2' ).insert( [ { id: 1, name: 'John', surname: 'J.' }, { id: 2, name: 'Max', surname: 'M.' }, { id: 3, name: 'George', surname: 'G.' }, { id: 4, name: 'Janet', surname: 'J.' }, { id: 5, name: 'Kate', surname: 'K.' } ] );
  assert.ok( insert.ok && insert.affected_rows === 5 , 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

});

it( 'Check', async() =>
{
  let check = await SQL( 'insert_users' ).get_all( 'id, name, description, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John', description: null, surname: null},
                                    { id: 2, name: 'Max', description: null, surname: null},
                                    { id: 3, name: 'George', description: null, surname: null},
                                    { id: 4, name: 'Janet', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

});
