'use strict';

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

module.exports.test = ( Logger ) =>
{
  return new Promise( async ( resolve, reject ) =>
  {
		await SQL('DROP TABLE IF EXISTS users').execute();
		await SQL('CREATE TABLE IF NOT EXISTS users ( id bigint unsigned NOT NULL AUTO_INCREMENT, name varchar(255) NOT NULL, PRIMARY KEY (id) )').execute( );

  	let insert = await SQL( 'users' ).insert( { name: 'john' } );

    Logger.log( 'Insert', insert );

    let select = await SQL( 'users' ).where( 'id = :?', insert.inserted_id ).get( '*' );

    Logger.log( 'Select', select );

    resolve( ( select.row ? select.row.name : 'err' ) );
  });
}

module.exports.expects = 'john';
