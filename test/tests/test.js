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
  await SQL('test_users').drop_table( true );

	let test_users = await SQL( {
		columns : {
				id      : { type: 'BIGINT:UNSIGNED', increment: true },
				name    : { type: 'VARCHAR:255', null: true },
				description : { type: 'TEXT', null: true },
				created : { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
				surname	: { type: 'VARCHAR:55', null: true, default: 'NULL' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : [ 'name' ]
		}
	}, 'test_users' ).create_table( true );

  await SQL( 'test_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' }, { name: 'Janet' }, { name: 'Max' }, { name: 'Janet' } ] );
});

it( 'Limit', async() =>
{
  let cnt = 0;
  let limit = await SQL( 'test_users' ).limit( 2 ).get_all('id,name');
  assert.ok( limit.ok && limit.rows && limit.rows.length === 2 , 'Limit '+ (++cnt) +' failed ' + JSON.stringify( limit, null, '  ' ) );

});

it( 'Offset', async() =>
{
  let cnt = 0;
  let offset = await SQL( 'test_users' ).limit( 2 ).offset( 2 ).order_by('id ASC').get_all('id,name');
  assert.deepEqual( offset.rows, [{ id: 3, name: 'George' }, { id: 4, name: 'Janet' }] , 'Offset '+ (++cnt) +' failed ' + JSON.stringify( offset, null, '  ' ) );

  offset = await SQL( 'test_users' ).offset( 3 ).order_by('id ASC').get_all('id,name');
  assert.deepEqual( offset.rows, [{ id: 4, name: 'Janet' }, { id: 5, name: 'Janet' }, { id: 6, name: 'Max' }, { id: 7, name: 'Janet' }] , 'Offset '+ (++cnt) +' failed ' + JSON.stringify( offset, null, '  ' ) );
});

it( 'Having', async() =>
{
  let cnt = 0;
  let having = await SQL( 'test_users' ).group_by( 'name' ).order_by('name ASC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
	assert.deepEqual( having.rows, [{ name: 'Janet', count: 3 },{ name: 'Max', count: 2 }] , 'Having '+ (++cnt) +' failed ' + JSON.stringify( having, null, '  ' ) );

  having = await SQL( 'test_users' ).order_by('name ASC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
  assert.deepEqual( having.rows, [{ name: 'Janet', count: 3 },{ name: 'Max', count: 2 }] , 'Having '+ (++cnt) +' failed ' + JSON.stringify( having, null, '  ' ) );
});

it( 'Select', async() =>
{
  let cnt = 0;

	let select = await SQL( 'test_users' ).where( 'created > :time OR name = :name ', { time: '( NOW() - INTERVAL 1 HOUR )', name: new Buffer( 'test', 'base64') }  ).order_by('name DESC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
	assert.deepEqual( select.rows, [{ name: 'Max', count: 2 },{ name: 'Janet', count: 3 }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );
});
