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
	},
  tables :
  {
    table_users : {
      columns : {
          id      : { type: 'BIGINT:UNSIGNED' },
          name    : { type: 'VARCHAR:255' },
          surname : { type: 'VARCHAR:255', default: '' }
      },
      indexes : {
        primary : 'id',
        unique  : 'name',
        index   : [ 'surname' ]
      }
    },
    table_address : {
      columns : {
          id      : { type: 'BIGINT:UNSIGNED' },
          name    : { type: 'VARCHAR:255' },
          city    : { type: 'VARCHAR:255', default: '' }
      },
      indexes : {
        primary : null,
        unique  : [ 'name' ],
        index   : [ 'id' ]
      }
    },
    table_cities : {
      columns : {
          id      : { type: 'BIGINT:UNSIGNED' },
          name    : { type: 'VARCHAR:255' },
          city    : { type: 'VARCHAR:255', default: '' }
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
  await SQL( 'DROP TABLE IF EXISTS table_users' ).execute();
  await SQL( 'DROP TABLE IF EXISTS table_address' ).execute();
  await SQL( 'DROP TABLE IF EXISTS table_cities' ).execute();
  await SQL( 'CREATE TABLE table_users ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, surname varchar(55) DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY name (name), KEY surname (surname) )' ).execute();
  await SQL( 'CREATE TABLE table_address ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, city varchar(55) DEFAULT NULL, UNIQUE KEY name (name), KEY id (id) )' ).execute();
  await SQL( 'CREATE TABLE table_cities ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, city varchar(55) DEFAULT NULL, UNIQUE KEY name (name), KEY id (id) )' ).execute();
  await SQL( 'table_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' } ] );
  await SQL( 'table_address' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
  await SQL( 'table_cities' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
});

it( 'Test', async() =>
{
  let cnt = 0;
  let test = await SQL( 'table_users' ).set( { id: 1, name: 'John D.' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'table_users' ).update( { name: 'Max', surname: 'M.' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'table_address' ).update( { name: 'John', city: 'New City' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

  test = await SQL( 'table_cities' ).update( { name: 'John', city: 'New City' } );
  assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );
});

it( 'Check', async() =>
{
  let check = await SQL( 'table_users' ).get_all( 'id, name, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John D.', surname: null},
                                    { id: 2, name: 'Max', surname: 'M.'},
                                    { id: 3, name: 'George', surname: null} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );
});
