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
		await SQL('DROP TABLE IF EXISTS update_users').execute();
		await SQL('CREATE TABLE IF NOT EXISTS update_users ( id bigint unsigned NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
		await SQL( 'update_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' } ] );

    let results = [];
    let update_1 = await SQL( 'update_users' ).update( { id: 1, name: 'John D.' } );
		let result_1 = await SQL( 'update_users' ).where( 'id = 1' ).get();
		results.push(  result_1.row );
    Logger.log( 'update_1', update_1 );

    let update_2 = await SQL( 'update_users' ).update([ { id: 2, name: 'Max M.' }, { id: 3, name: 'George G.' } ]);
		let result_2 = await SQL( 'update_users' ).where('id IN :?', [2,3] ).get_all();
		results.push( result_2.rows );
    Logger.log( 'update_2', update_2 );


    let update_3 = await SQL( 'update_users' ).update( [ { id: 4, name: 'Janet J.' } ] );
		let result_3 = await SQL( 'update_users' ).where('id = :?', 4 ).get('*');
		results.push( result_3.row );
    Logger.log( 'update_3', update_3 );

    let update_4 = await SQL( 'update_users' ).where( 'id = :?', 5 ).update( 'name = :?', 'Kate K.' );
		let result_4 = await SQL( 'update_users' ).where('id = :?', 5 ).get_all('*');
    results.push( result_4.rows );
    Logger.log( 'update_4', update_4 );

    resolve( ( results.length ? results : 'err' ) );
  });
}

module.exports.expects = [
	{ id: 1, name: 'John D.'},
	[{ id: 2, name: 'Max M.'}, { id: 3, name: 'George G.'}],
	{ id: 4, name: 'Janet J.'},
	[{ id: 5, name: 'Kate K.'}]
];
