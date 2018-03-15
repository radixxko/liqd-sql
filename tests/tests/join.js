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
		await SQL('DROP TABLE IF EXISTS join_users').execute();
    await SQL('DROP TABLE IF EXISTS join_address').execute();
		await SQL('CREATE TABLE IF NOT EXISTS join_users ( id bigint unsigned NOT NULL, name varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
		await SQL('CREATE TABLE IF NOT EXISTS join_address ( id bigint unsigned NOT NULL, active tinyint unsigned NOT NULL DEFAULT \'1\', city varchar(255) NOT NULL, PRIMARY KEY (id) )' ).execute();
		await SQL( 'join_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' } ] );
    await SQL( 'join_address' ).set( [ { id: 1, city: 'City' }, { id: 2, city: 'New' }, { id: 3, city: 'Old' } ] );

    let results = [];
    let result_1 = await SQL( 'join_users' )
      .join( 'join_address', 'join_address.id = join_users.id' )
      .where( 'join_address.id = 1' )
      .get('*');
		results.push(  result_1.row );
    Logger.log( 'result_1', result_1 );

    let result_2 = await SQL( 'join_users js' )
      .join( 'join_address ja', 'js.id = ja.id AND ja.active = 1' )
      .get_all('*');
		results.push(  result_2.rows );
    Logger.log( 'result_2', result_2 )

		let result_3 = await SQL( 'join_users js' )
		  .inner_join( 'join_address ja', 'js.id = ja.id AND ja.active = 1' )
		  .get_all('*');
		results.push(  result_3.rows );
		Logger.log( 'result_3', result_3 )

    resolve( ( results.length ? results : 'err' ) );
  });
}

module.exports.expects = [
	{ id: 1, name: 'John', active: 1, city: 'City' },
	[ { id: 1, name: 'John', active: 1, city: 'City' },
		{ id: 2, name: 'Max', active: 1, city: 'New' },
		{ id: 3, name: 'George', active: 1, city: 'Old' },
    { id: null, name: 'Janet', active: null, city: null },
    { id: null, name: 'Kate', active: null, city: null } ],
	[ { id: 1, name: 'John', active: 1, city: 'City' },
		{ id: 2, name: 'Max', active: 1, city: 'New' },
		{ id: 3, name: 'George', active: 1, city: 'Old' } ]
];
