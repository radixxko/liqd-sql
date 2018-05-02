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
  await SQL('union_users').drop_table( true );
  await SQL('union_address').drop_table( true );

	let union_users = await SQL( {
		columns : {
				id      : { type: 'BIGINT:UNSIGNED', increment: true },
				name    : { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : []
		}
	}, 'union_users' ).create_table( true );

	let union_address = await SQL( {
		columns : {
				id      : { type: 'BIGINT:UNSIGNED', increment: true },
				street	: { type: 'VARCHAR:255' },
				city    : { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : []
		}
	}, 'union_address' ).create_table( true );

	await SQL( 'union_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' } ] );
  await SQL( 'union_address' ).insert( [ { street: '5th', city: 'City' }, { street: 'Main', city: 'City' }, { street: 'Main', city: 'Paradise' }, { street: 'In', city: 'Paradise' }, { street: 'Second', city: 'Paradise' }  ] );
}).timeout(100000);

it( 'Union', async() =>
{
	let cnt = 0;
  let data1 = { id : 5, name: 'Kate' };
  let data2 = [{ name: 'Cooper' }, { id: 78, name: 'Iron' }];
	let data3 = [ { id : 5, name: 'Kate' }, { id: 78, name: 'Iron' } ];

	let union = await SQL( data1, 'alias' )
    .union( data2 )
    .union( SQL('union_users').where('name LIKE :?', 'John') )
    .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
    .union( SQL('union_users').where('name LIKE :?', 'Janet').columns( 'id, name' ) )
    .join( 'union_address a', 'alias.id = a.id' )
		.order_by( 'alias.name DESC' )
    .get_all( 'a.id :addressID, alias.name :name, a.street, a.city', { addressID: 'addressID', name: 'userName' } );
  assert.deepEqual( union.rows, [ { addressID: 5, userName: 'Kate', street: 'Second', city: 'Paradise' },
															{ addressID: 1, userName: 'John', street: '5th', city: 'City' },
															{ addressID: 4, userName: 'Janet', street: 'In', city: 'Paradise' },
															{ addressID: null, userName: 'Iron', street: null, city: null },
                              { addressID: 3, userName: 'George', street: 'Main', city: 'Paradise' },
                              { addressID: null, userName: 'Cooper', street: null, city: null }], 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

	union = await SQL( null, 'alias' )
		.union( [ data1, data2, SQL('union_users').where('name LIKE :?', 'John'), SQL('union_users').where('name LIKE :?', 'George').limit(1), SQL('union_users').where('name LIKE :?', 'Janet').columns( 'id, name' ) ] )
		.join( 'union_address a', 'alias.id = a.id' )
		.order_by( 'alias.name DESC' )
		.get_all( 'a.id :addressID, alias.name :name, a.street, a.city', { addressID: 'addressID', name: 'userName' } );
	assert.deepEqual( union.rows, [ { addressID: 5, userName: 'Kate', street: 'Second', city: 'Paradise' },
															{ addressID: 1, userName: 'John', street: '5th', city: 'City' },
															{ addressID: 4, userName: 'Janet', street: 'In', city: 'Paradise' },
															{ addressID: null, userName: 'Iron', street: null, city: null },
															{ addressID: 3, userName: 'George', street: 'Main', city: 'Paradise' },
															{ addressID: null, userName: 'Cooper', street: null, city: null }], 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

	union = await SQL( null, 'alias' )
		.union( [] )
		.join( 'union_address a', 'alias.id = a.id' )
		.order_by( 'alias.name DESC' )
		.get_all( 'a.id :addressID, alias.name :name, a.street, a.city', { addressID: 'addressID', name: 'userName' } );
	assert.ok( !union.ok && union.error && union.error.code === 'UNDEFINED_TABLE' , 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

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
                              { id: 5, street: 'Second', city: 'Paradise' }], 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

	union = await SQL( SQL('union_users').where('name LIKE :?', 'John'), 'alias' )
      .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
      .union( SQL('union_users u').where('u.name LIKE :?', 'Janet').columns( 'u.id id, u.name name' ) )
      .inner_join( 'union_address a', '1 = 1' )
			.union( data1 )
			.union( data3 )
			.limit(5)
      .get_all( 'a.id, a.street, a.city' );
  assert.deepEqual( union.rows, [{ id: 1, street: '5th', city: 'City' },
															{ id: 2, street: 'Main', city: 'City' },
                              { id: 3, street: 'Main', city: 'Paradise' },
                              { id: 4, street: 'In', city: 'Paradise' },
                              { id: 5, street: 'Second', city: 'Paradise' }], 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

  union = await SQL( data2 )
    .union( data1 )
    .union( SQL('union_users').where('name LIKE :?', 'John') )
    .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
    .union( SQL('union_users u').where('u.name LIKE :?', 'Janet').columns( 'u.id id, u.name name' ) )
    .where( 'id > 0' )
		.order_by( 'id DESC' )
    .get_all( 'name :name, id :id', { name: 'user_name', id: 'user_id' } );
  assert.deepEqual( union.rows, [ { user_name: 'Iron', user_id: 78 },
                                { user_name: 'Kate', user_id: 5 },
																{ user_name: 'Janet', user_id: 4 },
                                { user_name: 'George', user_id: 3 },
                                { user_name: 'John', user_id: 1 } ], 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

	union = await SQL( data1, 'uni-alias' )
      .union( data2 )
      .union( SQL('union_users').where('name LIKE :?', 'John') )
      .union( SQL('union_users').where('name LIKE :?', 'George').limit(1) )
      .union( SQL('union_users u').where('u.name LIKE :?', 'Janet').columns( 'u.id id' ) )
      .inner_join( 'union_address a', 'uni-alias.id = a.id' )
      .get_all( 'a.id, a.street, a.city' );
  assert.deepEqual( union.rows, [ { id: 1, street: '5th', city: 'City' },
                              { id: 3, street: 'Main', city: 'Paradise' },
                              { id: 4, street: 'In', city: 'Paradise' },
                              { id: 5, street: 'Second', city: 'Paradise' }], 'Union '+(++cnt)+' failed ' + JSON.stringify( union, null, '  ' ) );

}).timeout(100000);
