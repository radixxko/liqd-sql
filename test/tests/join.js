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
  await SQL( 'DROP TABLE IF EXISTS join_users').execute();
  await SQL( 'DROP TABLE IF EXISTS join_address').execute();
  await SQL( 'CREATE TABLE IF NOT EXISTS join_users ( id bigint unsigned NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
  await SQL( 'CREATE TABLE IF NOT EXISTS join_address ( id bigint unsigned NOT NULL, active tinyint unsigned NOT NULL DEFAULT \'1\', city varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
  await SQL( 'join_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' } ] );
  await SQL( 'join_users' ).set( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George G' }, { id: 4, name: 'Janet J' }, { id: 5, name: 'Kate K' } ] );
  await SQL( 'join_address' ).set( [ { id: 1, city: 'City' }, { id: 2, city: 'New' }, { id: 3, city: 'Old' } ] );
});

it( 'Join', async() =>
{
	let join = await SQL( 'join_users' )
    .join( 'join_address', 'join_address.id = join_users.id' )
    .where( 'join_address.id = 1' )
    .get('*').timeout( 1000 ).catch( e => e );
  assert.deepEqual( join.row, { id: 1, name: 'John', active: 1, city: 'City' }, 'Test error 1 failed ' + JSON.stringify( join, null, '  ' ) );

  join = await SQL( 'join_users js' )
    .join( 'join_address ja', 'js.id = ja.id AND ja.active = 1' )
    .get_all('*');
  assert.deepEqual( join.rows, [ { id: 1, name: 'John', active: 1, city: 'City' },
                                { id: 2, name: 'Max', active: 1, city: 'New' },
                                { id: 3, name: 'George G', active: 1, city: 'Old' },
                                { id: null, name: 'Janet J', active: null, city: null },
                                { id: null, name: 'Kate K', active: null, city: null } ], 'Test error 1 failed ' + JSON.stringify( join, null, '  ' ) );

  join = await SQL( 'join_users js' )
    .inner_join( 'join_address ja', 'js.id = ja.id AND ja.active = 1' )
    .get_all('*');
  assert.deepEqual( join.rows, [ { id: 1, name: 'John', active: 1, city: 'City' },
                                { id: 2, name: 'Max', active: 1, city: 'New' },
                                { id: 3, name: 'George G', active: 1, city: 'Old' } ], 'Test error 1 failed ' + JSON.stringify( join, null, '  ' ) );

});
