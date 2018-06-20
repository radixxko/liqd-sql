'use strict';

const assert = require('assert');
const SQL = new (require('../../lib/sql.js'))(
{
  mysql :
  {
    host            : 'localhost',
    user            : 'root',
    password        : '',
    database        : 'test'
  }
});

let insert, select, delete_row;

it( 'Create', async() =>
{
  await SQL.query( 'join_users').drop_table( true );
  await SQL.query( 'join_address').drop_table( true );

  let join_users = await SQL.query( {
    columns :
    {
      id          : { type: 'BIGINT:UNSIGNED' },
      name        : { type: 'VARCHAR:255' }
    },
    indexes : {
      primary : 'id',
      unique  : [],
      index   : []
    }
  }, 'join_users' ).create_table( true );

  let join_address = await SQL.query( {
    columns :
    {
      id      : { type: 'BIGINT:UNSIGNED' },
      active   : { type: 'TINYINT', default: 1 },
      city    : { type: 'VARCHAR:255' }
    },
    indexes : {
      primary : 'id',
      unique  : [],
      index   : []
    }
  }, 'join_address' ).create_table( true );

  await SQL.query( 'join_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' } ] );
  await SQL.query( 'join_users' ).set( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George G' }, { id: 4, name: 'Janet J' }, { id: 5, name: 'Kate K' } ] );
  await SQL.query( 'join_address' ).set( [ { id: 1, city: 'City' }, { id: 2, city: 'New' }, { id: 3, city: 'Old' } ] );


}).timeout(100000);

it( 'Join', async() =>
{
  let cnt = 0;
  let join = await SQL.query( 'join_users' )
    .join( 'join_address', 'join_address.id = join_users.id' )
    .where( 'join_address.id = 1' )
    .get('*');

  assert.deepEqual( join.row, { id: 1, name: 'John', active: 1, city: 'City' }, 'Test join '+(++cnt)+' failed ' + JSON.stringify( join, null, '  ' ) );

  join = await SQL.query( 'join_users js' )
    .join( 'join_address ja', 'js.id = ja.id AND ja.active = 1' )
    .get_all('*');
  assert.deepEqual( join.rows, [ { id: 1, name: 'John', active: 1, city: 'City' },
                                { id: 2, name: 'Max', active: 1, city: 'New' },
                                { id: 3, name: 'George G', active: 1, city: 'Old' },
                                { id: null, name: 'Janet J', active: null, city: null },
                                { id: null, name: 'Kate K', active: null, city: null } ], 'Test join '+(++cnt)+' failed ' + JSON.stringify( join, null, '  ' ) );

  join = await SQL.query( 'join_users js' )
    .inner_join( 'join_address ja', 'js.id = ja.id AND ja.active = 1' )
    .get_all('*');
  assert.deepEqual( join.rows, [ { id: 1, name: 'John', active: 1, city: 'City' },
                                { id: 2, name: 'Max', active: 1, city: 'New' },
                                { id: 3, name: 'George G', active: 1, city: 'Old' } ], 'Test join '+(++cnt)+' failed ' + JSON.stringify( join, null, '  ' ) );


}).timeout(100000);
