'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
let tables = require('./../all_tables.js');

//TOTO do configu to dat
const SQL = new (require('../../lib/sql.js'))( config );

let insert, select, delete_row;

it( 'Prepare', async() =>
{
	await SQL.query( 'table_users' ).drop_table( true );
	await SQL.query( 'table_address' ).drop_table( true );
	await SQL.query( 'table_cities' ).drop_table( true );
	await SQL.table( 'table_users_d' ).drop();
	await SQL.table( 'table_address_d' ).drop();
	await SQL.table( 'table_cities_d' ).drop();
}).timeout(100000);


it( 'Create', async() =>
{
	let cnt = 0;

	await SQL.query( tables[ 'table_users' ], 'table_users' ).create_table( true );
	await SQL.query( tables[ 'table_address' ], 'table_address' ).create_table( true );
	await SQL.table( tables[ 'table_cities' ], 'table_cities' ).create();

	let table_cities_old = await SQL.query( tables[ 'table_cities' ], 'table_cities' ).create_table();
	let table_cities_new = await SQL.table( tables[ 'table_cities' ], 'table_cities' ).create_query();

	let table_query = 'CREATE TABLE `table_cities` ( `id`  bigint(20) unsigned NOT NULL AUTO_INCREMENT, `name`  varchar(255) NOT NULL, `city`  varchar(255) NULL DEFAULT NULL,  UNIQUE KEY `name` (`name`),  KEY `id` (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_general_ci';

	assert.ok( table_cities_old === table_cities_new, 'Create query '+ (++cnt) +' failed ' );
	assert.ok( table_cities_new === table_query, 'Create query '+ (++cnt) +' failed ' );
}).timeout(100000);

it( 'Insert', async() =>
{
	await SQL.query( 'table_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' } ] );
	await SQL.query( 'table_address' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
	await SQL.query( 'table_cities' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );

}).timeout(100000);

it( 'Duplicate', async() =>
{
	let origin, duplicate, cnt = 0;

	let table_users = await SQL.table( 'table_users' ).duplicate( 'table_users_d' );
	let table_address = await SQL.table( 'table_address' ).duplicate( 'table_address_d', true );
	let table_cities = await SQL.table( 'table_cities' ).duplicate( 'table_cities_d', true );

	duplicate = await SQL.query( 'table_users_d' ).get( 'COUNT( * ) count' );
	assert.ok( duplicate.ok && duplicate.row.count === 0, 'Duplicate '+ (++cnt) +' failed ' );

	duplicate = await SQL.query( 'table_address_d' ).get( 'COUNT( * ) count' );
	origin = await SQL.query( 'table_address' ).get( 'COUNT( * ) count' );
	assert.ok( origin.ok && duplicate.ok && origin.row.count === duplicate.row.count , 'Duplicate '+ (++cnt) +' failed ' );

	duplicate = await SQL.query( 'table_cities_d' ).get( 'COUNT( * ) count' );
	origin = await SQL.query( 'table_cities' ).get( 'COUNT( * ) count' );
	assert.ok( origin.ok && duplicate.ok && origin.row.count === duplicate.row.count , 'Duplicate '+ (++cnt) +' failed ' )
}).timeout(100000);

it( 'Columns', async() =>
{
	let cnt = 0;
	let columns;
	columns = await SQL.table( 'table_users' ).columns();
	await config.compare_array( columns, [ 'id', 'name', 'surname' ], ++cnt, columns, 'check' );

	columns = await SQL.table( 'table_users_d' ).columns();
	await config.compare_array( columns, [ 'id', 'name', 'surname' ], ++cnt, columns, 'check' );
}).timeout(100000);

it( 'Schema', async() =>
{
	let cnt = 0;
	let schema;
	schema = await SQL.table( 'table_users' ).schema();
	await config.compare_objects( ( schema.ok ? schema.table : {} ), {
		columns :
		{
			id      : { type: 'BIGINT:20', unsigned: true, increment: true },
			name    : { type: 'VARCHAR:255' },
			surname : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [ 'name' ],
			index   : [ 'surname' ]
		}
	}, ++cnt, schema, 'check' );

	schema = await SQL.table( 'table_users_d' ).schema();
	await config.compare_objects( ( schema.ok ? schema.table : {} ), {
		columns :
		{
			id      : { type: 'BIGINT:20', unsigned: true, increment: true },
			name    : { type: 'VARCHAR:255' },
			surname : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [ 'name' ],
			index   : [ 'surname' ]
		}
	}, ++cnt, schema, 'check' );
}).timeout(100000);

it( 'Test', async() =>
{
	let cnt = 0;
	let test = await SQL.query( 'table_users' ).set( { id: 1, name: 'John D.' } );
	assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'table_users' ).set( { id: 1, name: 'John D.', surname: 'Doe' } );
	assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'table_users' ).update( { name: 'Max', surname: 'M.' } );
	assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'table_address' ).update( { name: 'John', city: 'New City' } );
	assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'table_cities' ).update( { name: 'John', city: 'New City' } );
	assert.ok( test.ok && test.affected_rows , 'Test '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );
}).timeout(100000);

it( 'Check', async() =>
{
	let cnt = 0;
	let check = await SQL.query( 'table_users' ).order_by('id ASC').get_all( 'id, name, surname' );

	let assert_check = await config.compare_array( check.rows, [ { id: 1, name: 'John D.', surname: 'Doe'},
		{ id: 2, name: 'Max', surname: 'M.'},
		{ id: 3, name: 'George', surname: null} ], ++cnt, check, 'check' );
}).timeout(100000);
