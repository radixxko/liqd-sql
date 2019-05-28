'use strict';

const assert = require('assert');

global.config = {
	mysql :
	{
		host     : 'localhost',
		user     : 'root',
		password : '',
		database : 'db0'
	},
	non_escape_keywords: [],
	copy : { database: 'db0', suffix: '_test' },
	tables : [ require('./tables_first.js'), require('./tables_second.js') ],
	all_tables : require('./all_tables.js'),
	connector : 'mysql',
	compare_objects: async function( origin, comparison, cnt, query, name = '' )
	{
		assert.ok( Object.values( origin ).length === Object.values( comparison ).length, 'Check compare_objects '+ cnt +' failed ' + JSON.stringify( { origin, comparison }, null, '  ' ) );

		for( let col_name in origin )
		{
			assert.ok( comparison.hasOwnProperty( col_name ) && ( ( comparison[ col_name ] && origin[ col_name ] === comparison[ col_name ] ) || ( ( !origin[ col_name ] || !comparison[ col_name ] ) && [null,''].includes(comparison[ col_name ]) && [null,''].includes(origin[ col_name ]) )), 'Compare object for '+ name + ' ' + cnt +' failed ' + JSON.stringify( {  origin, comparison }, null, '  ' ) );
		}
	},
	compare_array: async function( origin, comparison, cnt, query, name = '' )
	{
		assert.ok( origin.length === comparison.length, 'Check compare_array '+ cnt +' failed ' + JSON.stringify( query, null, '  ' ) );

		for( let i = 0; i < origin.length; i++ )
		{
			if( typeof origin[i] === 'object' )
			{
				await config.compare_objects( origin[i], comparison[i], cnt, query, name );
			}
		}
	}
};

const fs = require('fs');

describe( 'Tests', ( done ) =>
{
	var files = fs.readdirSync( __dirname + '/tests' );

	for( let file of files )
	{
		describe( file, () =>
		{
			require( __dirname + '/tests/' + file );
		});
	}

	setTimeout( () => { process.exit(); }, 10000000 );
});
