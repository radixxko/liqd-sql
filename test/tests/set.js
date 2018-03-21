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
	await SQL('DROP TABLE IF EXISTS set_address').execute();
  await SQL('CREATE TABLE set_users ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, description text NULL, created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, surname varchar(55) DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY name (name), KEY surname (surname) )').execute();
	await SQL('CREATE TABLE set_address ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, addressID bigint(20) unsigned NOT NULL, name varchar(255) NOT NULL, description text NULL, created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, city varchar(55) DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY name (addressID,name), KEY city (city) )').execute();
	await SQL( 'set_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' } ] );
	await SQL( 'set_address' ).insert( [ { id: 1, addressID: 1, name: 'Home' }, { id: 2, addressID: 2, name: 'Office' }, { id: 3, addressID: 3, name: 'Out' } ] );
});

it( 'Set', async() =>
{
  let cnt = 0;
  let set = await SQL( 'set_users' ).set( { id: 1, name: 'John D.' } );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

  set = await SQL( 'set_users' ).set([ { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.', description: { test: 'ok' } } ]);
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

  set = await SQL( 'set_users' ).set( [ { id: 4, name: 'Janet J.', description: null }, { name: 'Kate K.' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', description: 'null' }, { addressID: 2, name: 'Office', description: 'Main' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', '&description': '\'Values\'' }, { addressID: 2, name: 'Office', '!description': 'Main' }, { addressID: 1, name: 'Home', '?description': 'Nice' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

});

it( 'Check', async() =>
{
  let check = await SQL( 'set_users' ).get_all( 'id, name, description, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John D.', description: null, surname: null},
                                    { id: 2, name: 'Max M.', description: null, surname: null},
                                    { id: 3, name: 'George G.', description: '[object Object]', surname: null},
                                    { id: 4, name: 'Janet J.', description: null, surname: null },
                                    { id: 5, name: 'Kate K.', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

	check = await SQL( 'set_address' ).get_all( 'id,addressID, name, description' );

	assert.deepEqual( check.rows , [  { id: 1, addressID: 1, name: 'Home', description: null },
	                                  { id: 2, addressID: 2, name: 'Office', description: 'Main' },
	                                  { id: 3, addressID: 3, name: 'Out', description: 'Values'} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

});
