'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
const SQL = new (require('../../lib/sql.js'))( config );

it( 'Truncate', async() =>
{
	await SQL.query('truncate').drop_table( true );
	await SQL.query( config.all_tables['truncate'], 'truncate' ).create_table( true );

	for( let i = 1; i < 11; i++ )
	{
		await SQL.query( 'truncate' ).insert( { position: i } );
	}

	let cnt = 0;
	let inserted = await SQL.query( 'truncate' ).get_all();
	assert.ok( inserted.ok && inserted.rows.length === 10, 'Insert for truncate'+ (++cnt) +' failed' + JSON.stringify( inserted, null, '  ' ) );

	await SQL.query('truncate').truncate();

	inserted = await SQL.query( 'truncate' ).get_all();
	assert.ok( inserted.ok && inserted.rows.length === 0, 'Insert for truncate'+ (++cnt) +' failed' + JSON.stringify( inserted, null, '  ' ) );

}).timeout(100000);
