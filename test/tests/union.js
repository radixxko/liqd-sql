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
  await SQL('DROP TABLE IF EXISTS union_users').execute();
  await SQL('DROP TABLE IF EXISTS union_address').execute();
  await SQL('CREATE TABLE IF NOT EXISTS union_users ( id bigint unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
  await SQL('CREATE TABLE IF NOT EXISTS union_address ( id bigint unsigned NOT NULL AUTO_INCREMENT, street varchar(255) NOT NULL, city varchar(255) NOT NULL, PRIMARY KEY (id) )').execute();
  await SQL( 'union_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' } ] );
  await SQL( 'union_address' ).insert( [ { street: '5th', city: 'City' }, { street: 'Main', city: 'City' }, { street: 'Main', city: 'Paradise' }, { street: 'In', city: 'Paradise' }, { street: 'Second', city: 'Paradise' }  ] );
});

it( 'Union', async() =>
{
  let data1 = { id : 5, name: 'Kate' };
  let data2 = [{ name: 'Cooper' }, { id: 78, name: 'Iron' }];

	let union = await SQL( data1, 'alias' )
    .union( data2 )
    .union( SQL('union_users').where('name LIKE :?', 'John') )
    .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
    .union( SQL('union_users').where('name LIKE :?', 'Janet').columns( 'id, name' ) )
    .join( 'union_address a', 'alias.id = a.id' )
    .get_all( 'a.id :addressID, alias.name :name, a.street, a.city', { addressID: 'addressID', name: 'userName' } );
  assert.deepEqual( union.rows, [{ addressID: 1, userName: 'John', street: '5th', city: 'City' },
                              { addressID: 3, userName: 'George', street: 'Main', city: 'Paradise' },
                              { addressID: 4, userName: 'Janet', street: 'In', city: 'Paradise' },
                              { addressID: 5, userName: 'Kate', street: 'Second', city: 'Paradise' },
                              { addressID: null, userName: 'Cooper', street: null, city: null },
                              { addressID: null, userName: 'Iron', street: null, city: null }], 'Union 1 failed ' + JSON.stringify( union, null, '  ' ) );

  union = await SQL( data1, 'alias' )
      .union( data2 )
      .union( SQL('union_users').where('name LIKE :?', 'John') )
      .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
      .union( SQL('union_users u').where('u.name LIKE :?', 'Janet').columns( 'u.id id' ) )
      .inner_join( 'union_address a', 'alias.id = a.id' )
      .get_all( 'a.id, a.street, a.city' );
  assert.deepEqual( union.rows, [{ id: 1, street: '5th', city: 'City' },
                              { id: 3, street: 'Main', city: 'Paradise' },
                              { id: 4, street: 'In', city: 'Paradise' },
                              { id: 5, street: 'Second', city: 'Paradise' }], 'Union 2 failed ' + JSON.stringify( union, null, '  ' ) );

  union = await SQL( data2 )
    .union( data1 )
    .union( SQL('union_users').where('name LIKE :?', 'John') )
    .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
    .union( SQL('union_users u').where('u.name LIKE :?', 'Janet').columns( 'u.id id, u.name name' ) )
    .where( 'id > 0' )
    .get_all( 'name :name, id :id', { name: 'user_name', id: 'user_id' } );
  assert.deepEqual( union.rows, [ { user_name: 'Iron', user_id: 78 },
                                { user_name: 'Kate', user_id: 5 },
                                { user_name: 'John', user_id: 1 },
                                { user_name: 'George', user_id: 3 },
                                { user_name: 'Janet', user_id: 4 } ], 'Union 3 failed ' + JSON.stringify( union, null, '  ' ) );

});
