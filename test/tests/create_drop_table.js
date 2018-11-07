'use strict';

const assert = require('assert');
const SQL = new (require('../../lib/sql.js'))( config );

let insert, select, delete_row;

it( 'Create', async() =>
{
	await SQL.query( config.tables['create_user'] , 'create_user' ).create_table( true );

	let check = await SQL.query( 'create_user' ).insert( {  name: 'John' } );
	assert.ok( check.ok && check.affected_rows === 1, 'Create failed 1' + JSON.stringify( check, null, '	' ));

	await SQL.query( await SQL.query( config.tables['create_user_2'] , 'create_user_2' ).create_table() ).execute();
	check = await SQL.query( 'create_user_2' ).insert( { name: 'John' } );
	assert.ok( check.ok && check.affected_rows === 1, 'Create failed 2' + JSON.stringify( check, null, '	' ));
}).timeout(100000);

it( 'Drop', async() =>
{
	await SQL.query( 'create_user' ).drop_table( true );
	let check = await SQL.query( 'create_user' ).insert( { name: 'John' } );
	assert.ok( check.connector_error && check.connector_error.type === 'query', 'Drop failed 1' + JSON.stringify( check, null, '	' ));

	await SQL.query( SQL.query( 'create_user_2' ).drop_table() ).execute();
	check = await SQL.query( 'create_user_2' ).insert( { name: 'John' } );
	assert.ok( check.error && check.connector_error.type === 'query', 'Drop failed 2' + JSON.stringify( check, null, '	' ));
}).timeout(100000);
