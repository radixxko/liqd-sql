'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
const SQL = new (require('../../lib/sql.js'))( config );
let tables = require('./../all_tables.js');

const SLEEP = ( ms ) => new Promise( resolve => setTimeout( resolve, ms ));

async function generate_string( max_length, min_length = 0,length = null )
{
	if( length ){ max_length = length; }
	else if( max_length ){ max_length = Math.max( Math.floor( ( Math.random() * max_length ) ), min_length ); }

	let name = '';
	var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_+|~-=`!$%^&*(){}<>[]"\':;?,./abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz';

	for (var i = 0; i < max_length; i++)
	{
		name += chars[ Math.floor( Math.random() * chars.length ) ];
	}

	return name;
}

let insert;

it( 'Create', async() =>
{
	await SQL.query('insert_string').drop_table( true );
	await SQL.query('insert_users').drop_table( true );
	await SQL.query('insert_users_2').drop_table( true );
	await SQL.query('insert_users_3').drop_table( true );
	await SQL.query( tables['insert_string'], 'insert_string' ).create_table( true );
	await SQL.query( tables['insert_users'], 'insert_users' ).create_table( true );
	await SQL.query( tables['insert_users_2'], 'insert_users_2' ).create_table( true );
	await SQL.query( {
		columns :
		{
			id      : { type: 'BIGINT:UNSIGNED' },
			name    : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : []
		}
	}, 'insert_users_3' ).create_table( true );

}).timeout(100000);


async function randS( entry )
{
	return entry[ Math.floor( Math.random() * entry.length ) ];
}

it( 'Insert object', async() =>
{
	let cnt = 0;

	for( let i = 0; i < 1; i++ )
	{
		let data = {
			string1: await generate_string( 255, 10 ),
			string2: await generate_string( 255, 10 ),
			string3: await generate_string( 255, 10 ),
			string4: await generate_string( 12 ),
			string5: await generate_string( 3000, 500 ),
			string6: await generate_string( 15000, 100 ),
			string7: await randS( ['first','second','third'] ),
			string8: await randS( ['north','west','south','east'] ),
		};

		let insert = await SQL.query( 'insert_string' ).insert( data );

		assert.ok( insert.ok && insert.changed_rows === 1, 'Insert '+ (++cnt) +' failed' + JSON.stringify( insert, null, '  ' ) );
		await SLEEP( 10 );
	}
}).timeout(10000000);

it( 'Object insert', async() =>
{
	let cnt = 0;

	insert = await SQL.query( ).insert( [ { name: 'John' }, { name: 'Max' } ] );
	assert.ok( insert.error && insert.error.code === 'UNDEFINED_TABLE', 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users' ).insert( );
	assert.ok( insert.ok && insert.affected_rows === 0, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users' ).insert( [ { name: 'John' }, { name: 'Max' } ] );
	assert.ok( insert.ok && insert.affected_rows === 2, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users' ).insert( [ { name: 'George' }, { name: 'Janet' } ] );
	assert.ok( insert.ok && insert.affected_rows === 2 , 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users_2' ).insert( [ { id: 1, name: 'John', surname: 'J.' }, { id: 2, name: 'Max', surname: 'M.' }, { id: 3, name: 'George', surname: 'G.' }, { id: 4, name: 'Janet', surname: 'J.' }, { id: 5, name: 'Kate', surname: 'K.' } ] );
	assert.ok( insert.ok && insert.affected_rows === 5 , 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );
	assert.deepEqual(  insert.changed_id , { id: 1, name: 'John' }, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );
	assert.deepEqual(  insert.changed_ids , [{ id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' }], 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

	insert = await SQL.query( 'insert_users_3' ).insert( [ { id: 123456, name: 'John' } ] );
	assert.ok( insert.ok && insert.inserted_id === 123456, 'Insert '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );

}).timeout(10000000);

it( 'Check insert', async() =>
{
	let cnt = 0;
	let check = await SQL.query( 'insert_users' ).get_all( 'id, name, description, surname' );
	assert.ok( check.ok, 'Check '+ (++cnt) +' failed ' + JSON.stringify( insert, null, '  ' ) );
	let assert_check = await config.compare_array( check.rows, [  { id: 1, name: 'John', description: null, surname: null},
		{ id: 2, name: 'Max', description: null, surname: null},
		{ id: 3, name: 'George', description: null, surname: null},
		{ id: 4, name: 'Janet', description: null, surname: null } ], ++cnt, check );

}).timeout(100000);
