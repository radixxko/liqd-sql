'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
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
  await SQL('update_users').drop_table( true );
	await SQL('update_users_2').drop_table( true );

	await SQL( {
		columns : {
				id   	: { type: 'BIGINT:UNSIGNED' },
				uid		: { type: 'BIGINT:UNSIGNED' },
				name  : { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id',
			unique  : [ 'uid' ],
			index   : []
		}
	}, 'update_users' ).create_table( true );

	await SQL( {
		columns : {
				id   		: { type: 'BIGINT:UNSIGNED' },
				'u-id'	: { type: 'BIGINT:UNSIGNED' },
				name  	: { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : '',
			unique  : [],
			index   : []
		}
	}, 'update_users_2' ).create_table( true );

  await SQL( 'update_users' ).insert( [ { id: 1, uid: 1, name: 'John' }, { id: 2, uid: 2, name: 'Max' }, { id: 3, uid: 3, name: 'George' }, { id: 4, uid: 4, name: 'Janet' }, { id: 5, uid: 5, name: 'Kate' }, { id: 6, uid: 6, name: 'Tomas' } ] );
	await SQL( 'update_users_2' ).insert( [ { id: 1, 'u-id': 1, name: 'John' }, { id: 2, 'u-id': 2, name: 'Max' }, { id: 3, 'u-id': 3, name: 'George' }, { id: 4, 'u-id': 4, name: 'Janet' }, { id: 5, 'u-id': 5, name: 'Kate' }, { id: 6, 'u-id': 6, name: 'Tomas' } ] );
}).timeout(100000);

it( 'Update', async() =>
{
  let cnt = 0;
  let update = await SQL( ).update( { id: 1, name: 'John D.' } );
  assert.ok( update.error && update.error.code === 'UNDEFINED_TABLE' , 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );

	update = await SQL( 'update_users' ).update( );
  assert.ok( update.ok && update.affected_rows === 0 , 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );

	update = await SQL( 'update_users' ).update( { id: 1, name: 'John D.' } );
	assert.ok( update.ok && update.affected_rows === 1 , 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );

  update = await SQL( 'update_users' ).update([ { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.' } ]);
  assert.ok( update.ok && update.affected_rows  === 2, 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );

  update = await SQL( 'update_users' ).update( [ { id: 4, name: 'Janet J.' }, { uid: 6, name: 'Tomas T.' } ] );
  assert.ok( update.ok && update.affected_rows  === 2, 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );

  update = await SQL( 'update_users' ).where( 'id = :?', 5 ).update( 'name = :?', 'Kate K.' );
  assert.ok( update.ok && update.affected_rows  === 1, 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );

	update = await SQL( 'update_users_2' ).update( { id: 1, name: 'John D.' } );
  assert.ok( update.error && update.error.code === 'INVALID_ENTRY', 'Update '+ (++cnt) +' failed ' + JSON.stringify( update, null, '  ' ) );
}).timeout(100000);

it( 'Check', async() =>
{
  let check = await SQL( 'update_users' ).get_all();

  assert.deepEqual( check.rows , [  { id: 1, uid: 1, name: 'John D.'},
                                    { id: 2, uid: 2, name: 'Max M.'},
                                    { id: 3, uid: 3, name: 'George G.'},
                                    { id: 4, uid: 4, name: 'Janet J.' },
                                    { id: 5, uid: 5, name: 'Kate K.'},
																		{ id: 6, uid: 6, name: 'Tomas T.'} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

}).timeout(100000);
