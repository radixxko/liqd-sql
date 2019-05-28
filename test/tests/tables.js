'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
let tables = require('./../all_tables.js');

//TOTO do configu to dat
const SQL = new (require('../../lib/sql.js'))( config );

let insert, select, delete_row;

it( 'Create', async() =>
{
	await SQL.query( 'table_users' ).drop_table( true );
	await SQL.query( 'table_address' ).drop_table( true );
	await SQL.query( 'table_cities' ).drop_table( true );

	let table_users = await SQL.query( tables[ 'table_users' ], 'table_users' ).create_table( true );
	let table_address = await SQL.query( tables[ 'table_address' ], 'table_address' ).create_table( true );
	let table_cities = await SQL.query( tables[ 'table_cities' ], 'table_cities' ).create_table( true );

	await SQL.query( 'table_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' } ] );
	await SQL.query( 'table_address' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
	await SQL.query( 'table_cities' ).insert( [ { name: 'John', city: 'City' }, { name: 'Max', city: 'Paradise' } ] );
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
