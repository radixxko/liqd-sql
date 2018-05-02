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
	},
  tables :
  {
    table_users : {
      columns : {
          id      : { type: 'BIGINT:UNSIGNED', increment: true },
          name    : { type: 'VARCHAR:255' },
          surname : { type: 'VARCHAR:255', null: true, default: 'NULL' }
      },
      indexes : {
        primary : 'id',
        unique  : 'name',
        index   : [ 'surname' ]
      }
    },
    table_address : {
      columns : {
          id      : { type: 'BIGINT:UNSIGNED', increment: true },
          name    : { type: 'VARCHAR:255' },
          city    : { type: 'VARCHAR:255', null: true, default: 'NULL' }
      },
      indexes : {
        primary : null,
        unique  : [ 'name' ],
        index   : [ 'id' ]
      }
    },
    table_cities : {
      columns : {
          id      : { type: 'BIGINT:UNSIGNED', increment: true },
          name    : { type: 'VARCHAR:255' },
          city    : { type: 'VARCHAR:255', null: true, default: 'NULL' }
      },
      indexes : {
        primary : null,
        unique  : 'name',
        index   : [ 'id' ]
      }
    }
  }
});

let insert, select, delete_row;

it( 'Create', async() =>
{
  await SQL( 'table_users' ).drop_table( true );
  await SQL( 'table_address' ).drop_table( true );
  await SQL( 'table_cities' ).drop_table( true );

	let table_users = await SQL( {
		columns : {
				id      : { type: 'BIGINT:UNSIGNED', increment: true },
				name    : { type: 'VARCHAR:255' },
				surname : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes : {
			primary : 'id',
			unique  : 'name',
			index   : [ 'surname' ]
		}
	}, 'table_users' ).create_table( true );

	let table_address = await SQL( {
		columns : {
				id      : { type: 'BIGINT:UNSIGNED', increment: true },
				name    : { type: 'VARCHAR:255' },
				city    : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes : {
			primary : null,
			unique  : [ 'name' ],
			index   : [ 'id' ]
		}
	}, 'table_address' ).create_table( true );

	let table_cities = await SQL( {
		columns : {
				id      : { type: 'BIGINT:UNSIGNED', increment: true },
				name    : { type: 'VARCHAR:255' },
				city    : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes : {
			primary : null,
			unique  : 'name',
			index   : [ 'id' ]
		}
	}, 'table_cities' ).create_table( true );

	await SQL( 'table_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' } ] );
  await SQL( 'table_address' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
  await SQL( 'table_cities' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
}).timeout(100000);

it( 'Test', async() =>
{
  let cnt = 0;
  let test = await SQL( 'table_users' ).set( { id: 1, name: 'John D.' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL( 'table_users' ).set( { id: 1, name: 'John D.', surname: 'Doe' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'table_users' ).update( { name: 'Max', surname: 'M.' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'table_address' ).update( { name: 'John', city: 'New City' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'table_cities' ).update( { name: 'John', city: 'New City' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );
}).timeout(100000);

it( 'Check', async() =>
{
  let check = await SQL( 'table_users' ).get_all( 'id, name, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John D.', surname: 'Doe'},
                                    { id: 2, name: 'Max', surname: 'M.'},
                                    { id: 3, name: 'George', surname: null} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );
}).timeout(100000);
