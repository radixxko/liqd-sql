'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
const tables = require('../tables.js');

const SQL = new (require('../../lib/sql.js'))(
{
	mysql :
	{
		host     : 'localhost',
		user     : 'root',
		password : '',
		database : 'test'

	}
});

let insert, select, delete_row;

it( 'Create', async() =>
{
	await SQL.query('insert_users').drop_table( true );
	await SQL.query('insert_users_2').drop_table( true );
	await SQL.query( tables['insert_users'], 'insert_users' ).create_table( true );
	await SQL.query( tables['insert_users_2'], 'insert_users_2' ).create_table( true );
}).timeout(100000);

it( 'Insert', async() =>
{
	let cnt = 0;
	let insert = await SQL.query( ).insert( [ { name: 'John' }, { name: 'Max' } ] );
	assert.ok( insert.error && insert.error.code === 'UNDEFINED_TABLE', 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users' ).insert( );
	assert.ok( insert.ok && insert.affected_rows === 0, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users' ).insert( [ { name: 'John' }, { name: 'Max' } ] );
	assert.ok( insert.ok && insert.affected_rows === 2, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' } ], true );
	assert.ok( insert.ok && insert.affected_rows === 2 , 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users_2' ).insert( [ { id: 1, name: 'John', surname: 'J.' }, { id: 2, name: 'Max', surname: 'M.' }, { id: 3, name: 'George', surname: 'G.' }, { id: 4, name: 'Janet', surname: 'J.' }, { id: 5, name: 'Kate', surname: 'K.' } ] );
	assert.ok( insert.ok && insert.affected_rows === 5 , 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

}).timeout(10000000);

it( 'Check', async() =>
{
	let check = await SQL.query( 'insert_users' ).get_all( 'id, name, description, surname' );
	assert.deepEqual( check.rows , [  { id: 1, name: 'John', description: null, surname: null},
		{ id: 2, name: 'Max', description: null, surname: null},
		{ id: 3, name: 'George', description: null, surname: null},
		{ id: 4, name: 'Janet', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

}).timeout(100000);
