'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
const SQL = new (require('../../lib/sql.js'))( config );
let tables = require('./../all_tables.js');

let primary;

it( 'Primary', async() =>
{
	let cnt = 0;

	await SQL.query( 'primary_string').drop_table( true );
	await SQL.query( 'primary_number').drop_table( true );

	await SQL.query(  tables[ 'primary_string' ], 'primary_string').create_table( true );
	await SQL.query(  tables[ 'primary_number' ], 'primary_number').create_table( true );

	primary =  await SQL.query( 'primary_number' ).set( { id: 0, name: 'halo' } );
	assert.ok( primary.ok, 'Primary index '+ (++cnt) +' failed ' + JSON.stringify( primary, null, '  ' ) );

	primary =  await SQL.query( 'primary_number' ).set( { id: 0, name: 'halo edit' } );
	assert.ok( primary.ok, 'Primary index '+ (++cnt) +' failed ' + JSON.stringify( primary, null, '  ' ) );
}).timeout(100000);

it( 'Select', async() =>
{
	let cnt = 0;

	await SQL.query( 'selected').drop_table( true );
	await SQL.query( tables['selected'], 'selected' ).create_table( true );

	await SQL.query( 'selected2').drop_table( true );
	await SQL.query( tables['selected2'], 'selected2' ).create_table( true );

	await SQL.query( 'selected' ).insert( [ { id: 1, name: 'John`s' }, { id: 2, name: 'Max\'s' }, { id: 3, name: JSON.stringify( { name: 'Geo\\rge G' } ) }, { id: 4, name: 'Janet{\'asddada \'} J' }, { id: 5, name: '+++\\' } ] );

	let select = await SQL.query( 'selected' ).get_all( 'id, name, :? value', '' );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).get_all( 'id, name, :? value', 'exist' );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, name, :? value', 'exist' );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, name, :? value', '' );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, name, IF( name IS NULL, :exist, :empty ) value', { exist: 'exist', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, name, IF( name IS NULL, :exist, :string ) value', { exist: 'exist', string: 'empty' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, IF( name IS NULL, :exist, :string ) value, name, IF( name IS NULL, :exist, :string ) value', { exist: 'exist', string: 'empty' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, IF( name IS NOT NULL, :exist, :empty ) value, name, IF( name IS NULL, :exist, :empty ) value', { exist: 'exist', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, IF( name IS NULL, :exist, :empty ) value, name, IF( name IS NOT NULL, :exist, :string ) value', { exist: 'exist', empty: '', string: 'empty' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( 'id, IF( name IS NOT NULL, :exist, :empty ) value, name, IF( name IS NULL, :exist, :empty ) value', { exist: 'exist', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id' ).get_all( ':empty test, id, :string test, IF( name IS NULL, :exist, :string ) value, name, IF( name IS NULL, :exist, :empty ) value, :empty test', { exist: 'exist', string: 'NULL', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by( 'id, name' ).get_all( ':empty test, id, :string test, IF( name IS NULL, :exist, :string ) value, name, IF( name IS NULL, :exist, :empty ) value, :empty test', { exist: 'exist', string: 'NULL', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).join( 'selected2', 'selected2.id = selected.id AND selected.name != :?', '' ).group_by( 'selected.id, selected.name' ).get_all( ':empty test, selected.id, :string test, IF( selected2.name IS NULL, :exist, :string ) value, selected.name, IF( selected.name IS NULL, :exist, :empty ) value, :empty test', { exist: 'exist', string: 'NULL', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).get_all_query( 'id, name, :? value', 'exist',  'alias' );
	assert.ok( select && typeof select === 'string', 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).get_query( 'id, name, :? value', 'exist',  'alias' );
	assert.ok( select && typeof select === 'string', 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).get_query( 'id, name, :? value', 'exist' );
	assert.ok( select && typeof select === 'string', 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by('id').get_query( );
	assert.ok( select && typeof select === 'string', 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by('id').get();
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by('id').select_row_query( );
	assert.ok( select && typeof select === 'string', 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).group_by('id').select_row();
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).join( 'selected2', 'selected2.id = selected.id AND selected.name != :?', '' ).group_by( 'selected.id, selected.name' ).select( ':empty test, selected.id, :string test, IF( selected2.name IS NULL, :exist, :string ) value, selected.name, IF( selected.name IS NULL, :exist, :empty ) value, :empty test', { exist: 'exist', string: 'NULL', empty: '' } );
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected' ).select_query( 'id, name, :? value', 'exist',  'alias' );
	assert.ok( select && typeof select === 'string', 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'selected a' ).group_by('a.id').select( await SQL.query( 'a' ).escape_column() + '.' + await SQL.query( '*' ).escape_column() + ', ' + await SQL.query( 'test' ).escape_value() + ' test');
	assert.ok( select.ok, 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );


	select = await SQL.query( await SQL.query( 'selected s' )
			.where( ' ( ( name >= :name ) AND ( name <=  :test ) )', { name: 'string \\', test: 'string test' } )
			.group_by( 's.name' )
			.order_by( 's.name ASC' )
			.get_all_query( '*', null, 'a' )
		).get_all();

	assert.ok( select.ok , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

}).timeout(100000);
