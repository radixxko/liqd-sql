'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
let tables = require('./../all_tables.js');

const SQL = new (require('../../lib/sql.js'))({
	[ config.connector ] : config[ config.connector ],
	connector: config.connector,
	tables: JSON.parse(JSON.stringify(tables))
});

let insert, select, delete_row;

it( 'Create', async() =>
{
	await SQL.query('set_users').drop_table( true );
	await SQL.query('set_address').drop_table( true );
	await SQL.query('set_phones').drop_table( true );
	await SQL.query('set_names').drop_table( true );
	await SQL.query('set_test').drop_table( true );
	await SQL.query('set_numbers').drop_table( true );
	await SQL.query('set_string').drop_table( true );

	await SQL.query( tables['set_string'], 'set_string' ).create_table( true );
	await SQL.query( tables['set_users'], 'set_users' ).create_table( true );
	await SQL.query( tables['set_address'], 'set_address' ).create_table( true );
	await SQL.query( tables['set_phones'], 'set_phones' ).create_table( true );
	await SQL.query( tables['set_names'], 'set_names' ).create_table( true );
	await SQL.query({
		columns :
		{
			name : { type: 'VARCHAR:255', default: 'name' },
			uid  : { type: 'BIGINT', default: 'NULL', null: true }
		},
		indexes :
		{
			primary : '',
			unique  : ['name'],
			index   : []
		}
	}, 'set_test' ).create_table( true );
	await SQL.query( tables['set_numbers'], 'set_numbers' ).create_table( true );
}).timeout(100000);

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

async function randS( entry )
{
	return entry[ Math.floor( Math.random() * entry.length ) ];
}

it( 'Set object', async() =>
{
	let cnt = 0;

	for( let i = 0; i < 10; i++ )
	{
		insert = await SQL.query( 'insert_string' ).set(
			{
				uid : Math.ceil( Math.random() * 1099511627775 ),
				string1: await generate_string( 255, 10 ),
				string2: await generate_string( 255, 10 ),
				string3: await generate_string( 255, 10 ),
				string4: await generate_string( 12 ),
				string5: await generate_string( 3000, 500 ),
				string6: await generate_string( 15000, 100 ),
				string7: await randS( ['first','second','third'] ),
				string8: await randS( ['north','west','south','east'] ),
			});

		assert.ok( insert.ok && insert.changed_rows === 1, 'Insert '+ (++cnt) +' failed' + JSON.stringify( insert, null, '  ' ) );
	}
}).timeout(10000000);

it( 'Set', async() =>
{
	let cnt = 0, set = null;

	set = await SQL.query().set( { id: 1, name: 'John D.' } );
	assert.ok( set.error && set.error.code === 'UNDEFINED_TABLE' , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL.query( 'set_users' ).set( );
	assert.ok( set.ok && set.affected_rows === 0 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_users' ).set( [{ id: 1, name: 'John D.' }] );
	assert.ok( set.ok && set.inserted_id === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_users' ).set( [{ id: 1, name: 'John D.' }, { id: 3345678901231229, name: 'Max M.' }, { id: 3345678901231230, name: 'George G.', description: { test: 'ok' } } ] );
	assert.deepEqual( set.inserted_ids, [ 3345678901231229,3345678901231230 ], 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );
	assert.deepEqual( set.changed_ids, [ 3345678901231229,3345678901231230 ], 'Set '+ (cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	//set = await SQL.query( 'set_users' ).set( [ {  name: 'Janet J.', description: null }, { name: 'Kate K.' } ] );
	//assert.deepEqual( set.inserted_ids, [ 4,5 ], 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

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
	//assert.ok( set.ok && set.changed_rows === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) )

	//set = await SQL.query( 'set_users' ).set( { id: 40, name: 'Alex A.' } );
	//assert.ok( set.ok && set.changed_rows === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) )

	set = await SQL.query( 'set_phones' ).set( [ { userID: 3, phone: '12345' } ] );
	assert.ok( set.ok && set.affected_rows === 1 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_address' ).where( 'name = :?', 'Home' ).set( [ { id: 1, city: 'City' }, { id: 2, city: 'City' }, { id: 4, city: 'City' }, { id: 5, city: 'City' } ] );
	assert.ok( set.ok && set.affected_rows === 2 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_test' ).set( [ { name: 'John', uid: '123' } ] );
	assert.ok( set.ok && set.affected_rows === 1 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_test' ).set( [ { name: 'John', uid: '456' } ] );
	assert.ok( set.ok && set.affected_rows === 1 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_numbers' ).set( [ { id: '5354826364991516935', uid: '11160279303404754946' } ] );
	assert.ok( set.ok && set.affected_rows === 1 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

	set = await SQL.query( 'set_numbers' ).set( [ { id: '5354826364991516935', uid: '11160279303404754947' } ] );
	assert.ok( set.ok && set.affected_rows === 1 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, ' ' ) );

}).timeout(100000);


it( 'Check', async() =>
{
	let cnt = 0;
	let	check = await SQL.query( 'set_users' ).map('name').get_all( 'id, name, description, surname' );

	let assert_check = await config.compare_array( check.rows, [ { id: 1, name: 'John D.', description: null, surname: null},
		{ id: 3345678901231229, name: 'Max M.', description: null, surname: null},
		{ id: 3345678901231230, name: 'George G.', description: '{\"test\":\"ok\"}', surname: null},
	//	{ id: 4, name: 'Janet J.', description: null, surname: null },
	//	{ id: 5, name: 'Kate K.', description: null, surname: null },
	//	{ id: 40, name: 'Alex A.', description: null, surname: null }
		], ++cnt, check, 'check' );

	check = await SQL.query( 'set_address' ).order_by('id ASC').get_all( 'id,addressID, name, description, city' );

	assert_check = await config.compare_array( check.rows, [ { id: 1, addressID: 3, name: 'Out', description: 'Main', city: null },
		{ id: 2, addressID: 2, name: 'Office', description: 'Maintance', city: null },
		{ id: 3, addressID: 1, name: 'Home', description: 'Nice', city: null },
		{ id: 4, addressID: 4, name: 'Home', description: '', city: 'City' },
		{ id: 5, addressID: 5, name: 'Home', description: 'Nice', city: 'City' } ], ++cnt, check, 'check' );

}).timeout(100000);
