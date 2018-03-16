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
  await SQL('DROP TABLE IF EXISTS update_users').execute();
  await SQL('CREATE TABLE IF NOT EXISTS update_users ( id bigint unsigned NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
  await SQL( 'update_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' } ] );
});

it( 'Update', async() =>
{
  let cnt = 0;
  let update = await SQL( 'update_users' ).update( { id: 1, name: 'John D.' } );
  assert.ok( update.ok && update.affected_rows , 'Update '+ (cnt++) +' failed ' + JSON.stringify( update, null, '  ' ) );

  update = await SQL( 'update_users' ).update([ { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.' } ]);
  assert.ok( update.ok && update.affected_rows , 'Update '+ (cnt++) +' failed ' + JSON.stringify( update, null, '  ' ) );

  update = await SQL( 'update_users' ).update( [ { id: 4, name: 'Janet J.' } ] );
  assert.ok( update.ok && update.affected_rows , 'Update '+ (cnt++) +' failed ' + JSON.stringify( update, null, '  ' ) );

  update = await SQL( 'update_users' ).where( 'id = :?', 5 ).update( 'name = :?', 'Kate K.' );
  assert.ok( update.ok && update.affected_rows , 'Update '+ (cnt++) +' failed ' + JSON.stringify( update, null, '  ' ) );

});

it( 'Check', async() =>
{
  let check = await SQL( 'update_users' ).get_all();

  assert.deepEqual( check.rows , [  { id: 1, name: 'John D.'},
                                    { id: 2, name: 'Max M.'},
                                    { id: 3, name: 'George G.'},
                                    { id: 4, name: 'Janet J.' },
                                    { id: 5, name: 'Kate K.'} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

});
