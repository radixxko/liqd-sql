'use strict';

global.config = {
	mysql :
	{
		host     : 'localhost',
		user     : 'root',
		password : '',
		database : 'test'
	},
	tables : require('./tables.js'),
	connector : 'mysql'
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

	setTimeout( () => { process.exit(); }, 100000 );
});
