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
	await SQL.query('set_users').drop_table( true );
	await SQL.query('set_address').drop_table( true );
	await SQL.query('set_phones').drop_table( true );
	await SQL.query('set_names').drop_table( true );

	await SQL.query( tables['set_users'], 'set_users' ).create_table( true );
	await SQL.query( tables['set_address'], 'set_address' ).create_table( true );
	await SQL.query( tables['set_phones'], 'set_phones' ).create_table( true );
	await SQL.query( tables['set_names'], 'set_names' ).create_table( true );
}).timeout(100000);

it( 'Set', async() =>
{
	let cnt = 0, set = null;

	set = await SQL.query().set( { id: 1, name: 'John D.' } );
	assert.ok( set.error && set.error.code === 'UNDEFINED_TABLE' , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL.query( 'set_users' ).set( );
	assert.ok( set.ok && set.affected_rows === 0 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_users' ).set( [{ id: 1, name: 'John D.' }] );
	assert.ok( set.ok && set.inserted_id === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_users' ).set( [{ id: 1, name: 'John D.' }, { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.', description: { test: 'ok' } } ] );
	assert.deepEqual( set.inserted_ids, [ 2,3 ], 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_users' ).set( [ { id: 4, name: 'Janet J.', description: null }, { name: 'Kate K.' } ] );
	assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL.query( 'set_address' ).set( [ { addressID: 3, name: 'Out', description: 'null' }, { addressID: 2, name: 'Office', '!description': '' } ] );
	assert.ok( set.ok && set.affected_rows === 2  , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );
	assert.deepEqual( set.inserted_ids, [ 1,2 ], 'Set '+ (cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_address' ).set( [ { addressID: 3, name: 'Out', '&description': '\'Values\'' }, { addressID: 2, name: 'Office', '!description': 'Main' }, { addressID: 1, name: 'Home', '?description': 'Nice' } ] );
	assert.ok( set.ok && set.affected_rows === 3 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );
	assert.deepEqual( set.inserted_ids, [ 3 ], 'Set '+ (cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_address' ).set( [ { addressID: 3, name: 'Out', '?description': 'New' }, { addressID: 2, name: 'Office', '?description': { description: 'Main' } }, { addressID: 4, name: 'Home', '?&description': { description: '\'Nice\'' } }, { addressID: 5, name: 'Home', '?&description': { '&_default': '\'Nice\'' } } ] );
	assert.ok( set.ok && set.affected_rows === 4 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_address' ).set( [ { addressID: 3, name: 'Out', '?description': { '&Values': '\'Main\'' } }, { addressID: 2, name: 'Office', '?description': { 'Main': 'Maintance' } } ] );
	assert.ok( set.ok && set.affected_rows === 2, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_names' ).set( { id: 1, name: 'John' } );
	set = await SQL.query( 'set_names' ).set( { id: 10, name: 'John', surname: '' } );
	assert.ok( set.ok && set.changed_rows === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) )

	set = await SQL.query( 'set_users' ).set( { id: 40, name: 'Janet J.' }, { name: 'Kate K.', surname: 'K.' } );
	assert.ok( set.ok && set.changed_rows === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) )

	set = await SQL.query( 'set_phones' ).set( [ { userID: 3, phone: '12345' } ] );
	assert.ok( set.ok && set.affected_rows === 1 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_address' ).where( 'name = :?', 'Home' ).set( [ { id: 1, city: 'City' }, { id: 2, city: 'City' }, { id: 4, city: 'City' }, { id: 5, city: 'City' } ] );
	assert.ok( set.ok && set.affected_rows === 2 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );
}).timeout(100000);


it( 'Check', async() =>
{
	let check = await SQL.query( 'set_users' ).get_all( 'id, name, description, surname' );

	assert.deepEqual( check.rows , [  { id: 1, name: 'John D.', description: null, surname: null},
		{ id: 2, name: 'Max M.', description: null, surname: null},
		{ id: 3, name: 'George G.', description: '[object Object]', surname: null},
		{ id: 5, name: 'Kate K.', description: null, surname: null },
		{ id: 40, name: 'Janet J.', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

	check = await SQL.query( 'set_address' ).get_all( 'id,addressID, name, description' );

	let test = await SQL.query( 'set_address' ).get_all( 'id, name, city' );

	assert.deepEqual( check.rows , [  { id: 1, addressID: 3, name: 'Out', description: 'Main' },
		{ id: 2, addressID: 2, name: 'Office', description: 'Maintance' },
		{ id: 3, addressID: 1, name: 'Home', description: 'Nice'},
		{ id: 4, addressID: 4, name: 'Home', description: ''},
		{ id: 5, addressID: 5, name: 'Home', description: 'Nice'} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

}).timeout(100000);
