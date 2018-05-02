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
	}, 'create_user' ).create_table( true );

  let check = await SQL( 'create_user' ).insert( { name: 'John' } );
  assert.ok( check.ok && check.affected_rows === 1, 'Create failed 1' + JSON.stringify( check, null, '  ' ) );

  await SQL( await SQL({
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
	}, 'create_user_2' ).create_table() ).execute();

  check = await SQL( 'create_user_2' ).insert( { name: 'John' } );
  assert.ok( check.ok && check.affected_rows === 1, 'Create failed 2' + JSON.stringify( check, null, '  ' ) );
}).timeout(100000);

it( 'Drop', async() =>
{
  await SQL( 'create_user' ).drop_table( true );
  let check = await SQL( 'create_user' ).insert( { name: 'John' } );
  assert.ok( check.error && check.error.code === 'EREQUEST', 'Drop failed 1' + JSON.stringify( check, null, '  ' ) );

  await SQL( SQL( 'create_user_2' ).drop_table() ).execute();
  check = await SQL( 'create_user_2' ).insert( { name: 'John' } );
  assert.ok( check.error && check.error.code === 'EREQUEST', 'Drop failed 2' + JSON.stringify( check, null, '  ' ) );
}).timeout(100000);
