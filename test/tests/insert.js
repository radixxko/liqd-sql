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
  await SQL('DROP TABLE IF EXISTS insert_users').execute();
  let test = await SQL('CREATE TABLE insert_users ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, description text NULL, created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, surname varchar(55) DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY name (name), KEY surname (surname) )').execute();
});

it( 'Insert', async() =>
{
  let cnt = 0;
  let insert = await SQL( 'insert_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' } ] );
  insert = await SQL( 'insert_users' ).get_all() ;
  assert.ok( insert.ok && insert.inserted_ids, 'Insert '+ (cnt++) +' failed ' + JSON.stringify( insert, null, '  ' ) );

  insert = await SQL( 'insert_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' } ], 'ignore' );
  assert.ok( insert.ok && insert.inserted_ids && insert.inserted_ids.length === 2 , 'Insert '+ (cnt++) +' failed ' + JSON.stringify( insert, null, '  ' ) );

});

it( 'Check', async() =>
{
  let check = await SQL( 'insert_users' ).get_all( 'id, name, description, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John', description: null, surname: null},
                                    { id: 2, name: 'Max', description: null, surname: null},
                                    { id: 3, name: 'George', description: null, surname: null},
                                    { id: 4, name: 'Janet', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

});
