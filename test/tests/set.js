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
  await SQL('DROP TABLE IF EXISTS set_users').execute();
  let test = await SQL('CREATE TABLE set_users ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, description text NULL, created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, surname varchar(55) DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY name (name), KEY surname (surname) )').execute();
  await SQL( 'set_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' } ] );
});

it( 'Set', async() =>
{
  let cnt = 0;
  let set = await SQL( 'set_users' ).set( { id: 1, name: 'John D.' } );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (cnt++) +' failed ' + JSON.stringify( set, null, '  ' ) );

  set = await SQL( 'set_users' ).set([ { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.' } ]);
  assert.ok( set.ok && set.affected_rows , 'Set '+ (cnt++) +' failed ' + JSON.stringify( set, null, '  ' ) );

  set = await SQL( 'set_users' ).set( [ { id: 4, name: 'Janet J.' }, { name: 'Kate K.' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (cnt++) +' failed ' + JSON.stringify( set, null, '  ' ) );
});

it( 'Check', async() =>
{
  let check = await SQL( 'set_users' ).get_all( 'id, name, description, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John D.', description: null, surname: null},
                                    { id: 2, name: 'Max M.', description: null, surname: null},
                                    { id: 3, name: 'George G.', description: null, surname: null},
                                    { id: 4, name: 'Janet J.', description: null, surname: null },
                                    { id: 5, name: 'Kate K.', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

});
