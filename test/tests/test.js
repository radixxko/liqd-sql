'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
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
      active  : { type: 'TINYINT', default: 1 },
      city    : { type: 'VARCHAR:255' }
    },
    indexes : {
      primary : 'id',
      unique  : [],
      index   : []
    }
  }, 'join_address' ).create_table( true );

  await SQL.query( 'join_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George G' }, { id: 4, name: 'Janet J' }, { id: 5, name: 'Kate K' } ] );
  await SQL.query( 'join_address' ).insert( [ { id: 1, city: 'City' }, { id: 2, city: 'New' }, { id: 3, city: 'Old' } ] );
}).timeout(100000);


it( 'GROUP BY', async() =>
{
  //TODO test na group by aggregacne , ci moze byt v group by
  let interval = 360000;


  let cnt = 0;
  let test = await SQL.query( 'join_users js' )
          .inner_join( 'join_address', 'js.id = join_address.id AND join_address.active = 1' )
          .inner_join( 'join_address jas', 'js.id = jas.id AND jas.active = 1' )
          .group_by( 'js.name' )
          .order_by( 'js.name ASC' )
          .get_all('js.*, join_address.*');

  assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, city: 'Old' }, { id: 1, name: 'John', active: 1, city: 'City' },{ id: 2, name: 'Max', active: 1, city: 'New' }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  let part = 'adadd ( start DIV OR :? ) * status date';

  let alias = part.match(/\s+([\w]+)$/g);

  test = await SQL.query( 'set_address')
  .where('label = :label AND start >= :start', { label: 'AAAA', start: 10000 })
  .group_by('start DIV :? , status', interval)
  .order_by('start DIV :? ASC', interval)
  .get_all_query('COUNT( DISTINCT label ) cnt, ( `start` DIV :? ) * status date, start + start alias, IF( ( start * end ) > 0, 1, 2 ), status', interval );

  assert.ok( test === "SELECT COUNT( DISTINCT `label` ) `cnt`,  ( MAX(`start`) DIV 360000 ) * MAX(`status`) `date`,  MAX(`start`) + MAX(`start`) `alias`,  IF( ( MAX(`start`) * MAX(`end`) ) > 0,  1,  2 ),  MAX(`status`) `status` FROM `set_address` WHERE `label` = 'AAAA' AND `start` >= 10000 GROUP BY `start` DIV 360000 , `status` ORDER BY `start` DIV 360000 ASC" , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL.query( 'set_address')
  .where('label = :label AND start >= :start', { label: 'AAAA', start: 10000 })
  .group_by('start DIV :? , status', interval)
  .order_by('start DIV :? ASC', interval)
  .get_all_query('COUNT( DISTINCT street, city ) cnt, ( `start` DIV :? ) * status date, start + start alias, IF( ( start * end ) > 0, 1, 2 ), status', interval );

  assert.equal( test.replace(/\s+/g,' ') , "SELECT COUNT( DISTINCT `street`, `city` ) `cnt`,  ( MAX(`start`) DIV 360000 ) * MAX(`status`) `date`,  MAX(`start`) + MAX(`start`) `alias`,  IF( ( MAX(`start`) * MAX(`end`) ) > 0,  1,  2 ),  MAX(`status`) `status` FROM `set_address` WHERE `label` = 'AAAA' AND `start` >= 10000 GROUP BY `start` DIV 360000 , `status` ORDER BY `start` DIV 360000 ASC".replace(/\s+/g,' ') , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );


  test = await SQL.query( 'join_users      js' )
        .inner_join( SQL.query( 'join_address', 'aa' ).group_by( 'id' ).columns( 'join_address.id, COUNT(*) count, active' ), 'js.id = aa.id AND aa.active = 1' )
        .group_by( 'js.name' )
        .order_by( 'js.name ASC' )
        .get_all('*');

  assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, count: 1 }, { id: 1, name: 'John', active: 1, count: 1 },{ id: 2, name: 'Max', active: 1, count: 1 }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );


  test = await SQL.query( 'join_users js' )
          .inner_join( SQL.query( 'join_address', 'aa' ).group_by( 'id' ).columns( '*' ), 'js.id = aa.id AND aa.active = 1' )
          .inner_join( 'join_address jas', 'js.id = jas.id AND jas.active = 1' )
          .group_by( 'js.name' )
          .order_by( 'js.name ASC' )
          .get_all('*');
  assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, city: 'Old' }, { id: 1, name: 'John', active: 1, city: 'City' },{ id: 2, name: 'Max', active: 1, city: 'New' }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );


  test = await SQL.query( 'join_users js' )
           .inner_join( SQL.query( 'join_address', 'aa' ).group_by( 'id' ).columns( 'join_address.id, active' ), 'js.id = aa.id AND aa.active = 1' )
           .inner_join( 'join_address jas', 'js.id = jas.id AND jas.active = 1' )
           .group_by( 'js.name' )
           .order_by( 'js.name ASC' )
           .get_all('*');
  assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, city: 'Old' }, { id: 1, name: 'John', active: 1, city: 'City' },{ id: 2, name: 'Max', active: 1, city: 'New' }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );
}).timeout(100000);

it( 'Create', async() =>
{
  await SQL.query('test_users').drop_table( true );

  let test_users = await SQL.query( {
    columns : {
        id      : { type: 'BIGINT:UNSIGNED', increment: true },
        name    : { type: 'VARCHAR:255', null: true },
        description : { type: 'TEXT', null: true },
        created : { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
        surname  : { type: 'VARCHAR:55', null: true, default: 'NULL' }
    },
    indexes : {
      primary : 'id',
      unique  : [],
      index   : [ 'name' ]
    }
  }, 'test_users' ).create_table( true );

  await SQL.query( 'test_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' }, { name: 'Janet' }, { name: 'Max' }, { name: 'Janet' } ] );
}).timeout(100000);

it( 'Where', async() =>
{
  let cnt = 0;
  let where = await SQL.query( 'test_users' ).where( [ { id: 1 }, { id: 2 } ] ).limit( 2 ).select('id,name');
  assert.ok( where.ok && where.rows && where.rows.length === 2 , 'Where '+ (++cnt) +' failed ' + JSON.stringify( where, null, '  ' ) );

}).timeout(100000);

it( 'Limit', async() =>
{
  let cnt = 0;
  let limit = await SQL.query( 'test_users' ).limit( 2 ).select('id,name');
  assert.ok( limit.ok && limit.rows && limit.rows.length === 2 , 'Limit '+ (++cnt) +' failed ' + JSON.stringify( limit, null, '  ' ) );

}).timeout(100000);

it( 'Offset', async() =>
{
  let cnt = 0;
  let offset = await SQL.query( 'test_users' ).limit( 2 ).offset( 2 ).order_by('id ASC').get_all('id,name');
  assert.deepEqual( offset.rows, [{ id: 3, name: 'George' }, { id: 4, name: 'Janet' }] , 'Offset '+ (++cnt) +' failed ' + JSON.stringify( offset, null, '  ' ) );

  offset = await SQL.query( 'test_users' ).offset( 3 ).order_by('id ASC').get_all('id,name');
  assert.deepEqual( offset.rows, [{ id: 4, name: 'Janet' }, { id: 5, name: 'Janet' }, { id: 6, name: 'Max' }, { id: 7, name: 'Janet' }] , 'Offset '+ (++cnt) +' failed ' + JSON.stringify( offset, null, '  ' ) );
}).timeout(100000);

it( 'Having', async() =>
{
  let cnt = 0;
  let having = await SQL.query( 'test_users' ).group_by( 'name' ).order_by('name ASC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
  assert.deepEqual( having.rows, [{ name: 'Janet', count: 3 },{ name: 'Max', count: 2 }] , 'Having '+ (++cnt) +' failed ' + JSON.stringify( having, null, '  ' ) );

  having = await SQL.query( 'test_users' ).order_by('name ASC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
  assert.deepEqual( having.rows, [{ name: 'Max', count: 7 }] , 'Having '+ (++cnt) +' failed ' + JSON.stringify( having, null, '  ' ) );
}).timeout(100000);

it( 'Select', async() =>
{
  let cnt = 0;

  let select = await SQL.query( 'test_users' ).where( 'created > :time OR name = :name ', { time: '( NOW() - INTERVAL 1 HOUR )', name: new Buffer( 'test', 'base64') }  ).order_by('name DESC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
  assert.deepEqual( select.rows, [{ name: 'Max', count: 7 }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );
}).timeout(100000);
