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
	}
});

let insert, select, delete_row;

it( 'Create', async() =>
{
	await SQL('set_users').drop_table( true );
	await SQL('set_address').drop_table( true );
	await SQL('set_phones').drop_table( true );

	await SQL( {
		columns :
		{
			id      		: { type: 'BIGINT:UNSIGNED', increment: true },
			name    		: { type: 'VARCHAR:255' },
			description	: { type: 'TEXT', null: true },
			created 		: { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			surname    	: { type: 'VARCHAR:55', null: true }
		},
		indexes : {
			primary : 'id',
			unique  : 'name',
			index   : [ 'surname' ]
		}
	}, 'set_users' ).create_table( true );

	await SQL( {
		columns :
		{
			id      		: { type: 'BIGINT:UNSIGNED', increment: true },
			addressID   : { type: 'BIGINT:UNSIGNED' },
			name    		: { type: 'VARCHAR:255' },
			description	: { type: 'TEXT', null: true },
			created 		: { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			city    		: { type: 'VARCHAR:55', null: true }
		},
		indexes : {
			primary : 'id',
			unique  : 'addressID,name',
			index   : [ 'city' ]
		}
	}, 'set_address' ).create_table( true );

	await SQL( {
		columns :
		{
			userID      : { type: 'BIGINT:UNSIGNED' },
			phone   		: { type: 'BIGINT:UNSIGNED' }
		},
		indexes : {
			primary : '',
			unique  : [ ],
			index   : [ ]
		}
	}, 'set_phones' ).create_table( true );

	//await SQL( 'set_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' } ] );
	//await SQL( 'set_address' ).insert( [ { addressID: 1, name: 'Home' }, { addressID: 2, name: 'Office' }, { addressID: 3, name: 'Out' } ] );
});

it( 'Set', async() =>
{
  let cnt = 0, set = null;

	set = await SQL().set( { id: 1, name: 'John D.' } );
  assert.ok( set.error && set.error.code === 'UNDEFINED_TABLE' , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_users' ).set( );
  assert.ok( set.ok && set.affected_rows === 0 , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_users' ).set( [{ id: 1, name: 'John D.' }] );
  assert.ok( set.ok && set.inserted_id === 1, 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_users' ).set( [{ id: 1, name: 'John D.' }, { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.', description: { test: 'ok' } } ] );
	assert.deepEqual( set.inserted_ids, [ 2,3 ], 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_users' ).set( [ { id: 4, name: 'Janet J.', description: null }, { name: 'Kate K.' } ] );
	assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', description: 'null' }, { addressID: 2, name: 'Office', description: 'Main' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', '&description': '\'Values\'' }, { addressID: 2, name: 'Office', '!description': 'Main' }, { addressID: 1, name: 'Home', '?description': 'Nice' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', '?description': 'New' }, { addressID: 2, name: 'Office', '?description': { description: 'Main' } }, { addressID: 4, name: 'Home', '?&description': { description: '\'Nice\'' } }, { addressID: 5, name: 'Home', '?&description': { '&_default': '\'Nice\'' } } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', '?description': { '&New': 'Main' } }, { addressID: 2, name: 'Office', '?description': { 'Main': 'Maintance' } } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_phones' ).set( [ { userID: 3, phone: '12345' } ] );
	assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	/*
  set = await SQL( 'set_users' ).set([ { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.', description: { test: 'ok' } } ]);
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );




	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', '&description': '\'Values\'' }, { addressID: 2, name: 'Office', '!description': 'Main' }, { addressID: 4, name: 'Homes', 'description': 'asa' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );

	set = await SQL( 'set_address' ).set( [ { addressID: 3, name: 'Out', '&description': '\'Values\'' }, { addressID: 2, name: 'Office', '!description': 'Main' }, { addressID: 1, name: 'Home', '?&description': '\'Nice\'' } ] );
  assert.ok( set.ok && set.affected_rows , 'Set '+ (++cnt) +' failed ' + JSON.stringify( set, null, '  ' ) );


	*/
});

/*
it( 'Check', async() =>
{
  let check = await SQL( 'set_users' ).get_all( 'id, name, description, surname' );

  assert.deepEqual( check.rows , [  { id: 1, name: 'John D.', description: null, surname: null},
                                    { id: 2, name: 'Max M.', description: null, surname: null},
                                    { id: 3, name: 'George G.', description: '[object Object]', surname: null},
                                    { id: 4, name: 'Janet J.', description: null, surname: null },
                                    { id: 5, name: 'Kate K.', description: null, surname: null } ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

	check = await SQL( 'set_address' ).get_all( 'id,addressID, name, description' );

	assert.deepEqual( check.rows , [  { id: 1, addressID: 1, name: 'Home', description: null },
	                                  { id: 2, addressID: 2, name: 'Office', description: 'Main' },
	                                  { id: 3, addressID: 3, name: 'Out', description: 'Values'} ], 'Check failed ' + JSON.stringify( check, null, '  ' ) );

});
*/
