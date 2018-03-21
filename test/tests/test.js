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
  await SQL('DROP TABLE IF EXISTS test_users').execute();
  let test = await SQL('CREATE TABLE test_users ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, description text NULL, created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, surname varchar(55) DEFAULT NULL, PRIMARY KEY (id), KEY name (name) )').execute();
  await SQL( 'test_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Janet' }, { id: 6, name: 'Max' }, { id: 7, name: 'Janet' } ] );
});

it( 'Limit', async() =>
{
  let cnt = 0;
  let limit = await SQL( 'test_users' ).limit( 2 ).get_all('id,name');
  assert.ok( limit.ok && limit.rows && limit.rows.length === 2 , 'Limit '+ (cnt++) +' failed ' + JSON.stringify( limit, null, '  ' ) );

});

it( 'Offset', async() =>
{
  let cnt = 0;
  let offset = await SQL( 'test_users' ).limit( 2 ).offset( 2 ).order_by('id ASC').get_all('id,name');
  assert.deepEqual( offset.rows, [{ id: 3, name: 'George' }, { id: 4, name: 'Janet' }] , 'Offset '+ (cnt++) +' failed ' + JSON.stringify( offset, null, '  ' ) );

  offset = await SQL( 'test_users' ).offset( 3 ).order_by('id ASC').get_all('id,name');
  assert.deepEqual( offset.rows, [{ id: 4, name: 'Janet' }, { id: 5, name: 'Janet' }, { id: 6, name: 'Max' }, { id: 7, name: 'Janet' }] , 'Offset '+ (cnt++) +' failed ' + JSON.stringify( offset, null, '  ' ) );
});

it( 'Having', async() =>
{
  let cnt = 0;
  let having = await SQL( 'test_users' ).group_by( 'name' ).order_by('name ASC').having( 'count > 1' ).get_all('name, COUNT(*) count');
  assert.deepEqual( having.rows, [{ name: 'Janet', count: 3 },{ name: 'Max', count: 2 }] , 'Having '+ (cnt++) +' failed ' + JSON.stringify( having, null, '  ' ) );

  having = await SQL( 'test_users' ).order_by('name ASC').having( 'count > 1' ).get_all('name, COUNT(*) count');
  assert.deepEqual( having.rows, [{ name: 'Janet', count: 3 },{ name: 'Max', count: 2 }] , 'Having '+ (cnt++) +' failed ' + JSON.stringify( having, null, '  ' ) );
});
