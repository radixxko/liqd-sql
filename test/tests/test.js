'use strict';

const assert = require('assert');
const TimedPromise = require('liqd-timed-promise');
let SQL = new (require('../../lib/sql.js'))( config );

SQL.on( 'query', (query) =>
{
});

SQL.on( 'before-query', (query) =>
{
	//console.log( 'SQL before query', query );
});

SQL.off( 'before-query', (query) =>
{
	//console.log( 'SQL before query', query );
});

it( 'Connected', async() =>
{
	let database_status = SQL.connected;
	assert.ok( database_status, true, 'Connected failed ' + JSON.stringify( database_status, null, '  ' ) );
}).timeout(100000);

it( 'Create', async() =>
{
	await SQL.query( 'join_users').drop_table( true );
	await SQL.query( 'join_address').drop_table( true );

	let join_users = await SQL.query( config.tables['join_users'], 'join_users' ).create_table( true );
	let join_address = await SQL.query( config.tables['join_address'], 'join_address' ).create_table( true );

	await SQL.query( 'join_users' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George G' }, { id: 4, name: 'Janet J' }, { id: 5, name: 'Kate K' } ] );
	await SQL.query( 'join_address' ).insert( [ { id: 1, city: 'City' }, { id: 2, city: 'New' }, { id: 3, city: 'Old' } ] );
}).timeout(100000);

it( 'GROUP BY', async() =>
{
	let interval = 360000;

	let cnt = 0;
	let test = await SQL.query( 'join_users js' )
		.inner_join( 'join_address', 'js.id = join_address.id AND join_address.active = 1' )
		.inner_join( 'join_address jas', 'js.id = jas.id AND jas.active = 1' )
		.group_by( 'js.name' )
		.order_by( 'js.name ASC' )
		.get_all('js.*, join_address.*');
	assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, city: 'Old' }, { id: 1, name: 'John', active: 1, city: 'City' },{ id: 2, name: 'Max', active: 1, city: 'New' }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	let part = 'adadd ( start DIV OR :? ) * status date';
	let alias = part.match(/\s+([\w]+)$/g);

	let expected_string = "SELECT COUNT( DISTINCT `label` ) `cnt`,  ( MAX(`start`) DIV 360000 ) * MAX(`status`) `date`,  MAX(`start`) + MAX(`start`) `alias`,  IF( ( MAX(`start`) * MAX(`end`) ) > 0,  1,  2 ),  MAX(`status`) `status` FROM `set_address` WHERE `label` = 'AAAA' AND `start` >= 10000 GROUP BY `start` DIV 360000 , `status` ORDER BY `start` DIV 360000 ASC";
	test = await SQL.query( 'set_address')
		.where('label = :label AND start >= :start', { label: 'AAAA', start: 10000 })
		.group_by('start DIV :? , status', interval)
		.order_by('start DIV :? ASC', interval)
		.get_all_query('COUNT( DISTINCT label ) cnt, ( start DIV :? ) * status date, start + start alias, IF( ( start * end ) > 0, 1, 2 ), status', interval );

	test = test.replace(/"/g,'`').replace(/(IIF)/g,'IF');
	assert.equal( test.replace(/\s+/g,' ') , expected_string.replace(/\s+/g,' '), 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	expected_string = "SELECT COUNT( DISTINCT `street`, `city` ) `cnt`,  ( MAX(`start`) DIV 360000 ) * MAX(`status`) `date`,  MAX(`start`) + MAX(`start`) `alias`,  IF( ( MAX(`start`) * MAX(`end`) ) > 0,  1,  2 ),  MAX(`status`) `status` FROM `set_address` WHERE `label` = 'AAAA' AND `start` >= 10000 GROUP BY `start` DIV 360000 , `status` ORDER BY `start` DIV 360000 ASC";
	test = await SQL.query( 'set_address')
		.where('label = :label AND start >= :start', { label: 'AAAA', start: 10000 })
		.group_by('start DIV :? , status', interval)
		.order_by('start DIV :? ASC', interval)
		.get_all_query('COUNT( DISTINCT street, city ) cnt, ( start DIV :? ) * status date, start + start alias, IF( ( start * end ) > 0, 1, 2 ), status', interval );

	test = test.replace(/"/g,'`').replace(/(IIF)/g,'IF');
	assert.equal( test.replace(/\s+/g,' ') , expected_string.replace(/\s+/g,' ') , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'join_users      js' )
		.inner_join( SQL.query( 'join_address', 'aa' ).group_by( 'id' ).columns( 'join_address.id, COUNT(*) count, active' ), 'js.id = aa.id AND aa.active = 1' )
		.group_by( 'js.name' )
		.order_by( 'js.name ASC' )
		.get_all('*');
	assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, count: 1 }, { id: 1, name: 'John', active: 1, count: 1 },{ id: 2, name: 'Max', active: 1, count: 1 }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'join_users js' )
		.inner_join( SQL.query( 'join_address', 'aa' ).group_by( 'id' ).columns( '*' ), 'js.id = aa.id AND aa.active = 1' )
		.inner_join( 'join_address jas', 'js.id = jas.id AND jas.active = 1' )
		.group_by( 'js.name' )
		.order_by( 'js.name ASC' )
		.get_all('*');
	assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, city: 'Old' }, { id: 1, name: 'John', active: 1, city: 'City' },{ id: 2, name: 'Max', active: 1, city: 'New' }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

	test = await SQL.query( 'join_users js' )
		.inner_join( SQL.query( 'join_address', 'aa' ).group_by( 'id' ).columns( 'join_address.id, active' ), 'js.id = aa.id AND aa.active = 1' )
		.inner_join( 'join_address jas', 'js.id = jas.id AND jas.active = 1' )
		.group_by( 'js.name' )
		.order_by( 'js.name ASC' )
		.get_all('*');
	assert.deepEqual( test.rows, [{ id: 3, name: 'George G', active: 1, city: 'Old' }, { id: 1, name: 'John', active: 1, city: 'City' },{ id: 2, name: 'Max', active: 1, city: 'New' }] , 'GROUP BY '+ (++cnt) +' failed ' + JSON.stringify( test, null, '  ' ) );

}).timeout(100000);

it( 'Create', async() =>
{
	await SQL.query('test_users').drop_table( true );
	let test_users = await SQL.query( config.tables['test_users'], 'test_users' ).create_table( true );
	await SQL.query( 'test_users' ).insert( [ { name: 'John' }, { name: 'Max' }, { name: 'George' }, { name: 'Janet' }, { name: 'Janet' }, { name: 'Max' }, { name: 'Janet' } ] );

}).timeout(100000);

it( 'Where', async() =>
{
	let cnt = 0;
	let where = await SQL.query( 'test_users' ).where( [ { id: 1 }, { id: 2 } ] ).limit( 2 ).select('id,name');
	assert.ok( where.ok && where.rows && where.rows.length === 2 , 'Where '+ (++cnt) +' failed ' + JSON.stringify( where, null, '  ' ) );

}).timeout(100000);

it( 'Limit', async() =>
{
	let cnt = 0;
	let limit = await SQL.query( 'test_users' ).limit( 2 ).select('id,name');
	assert.ok( limit.ok && limit.rows && limit.rows.length === 2 , 'Limit '+ (++cnt) +' failed ' + JSON.stringify( limit, null, '  ' ) );

}).timeout(100000);

it( 'Offset', async() =>
{
	let cnt = 0;
	let offset = await SQL.query( 'test_users' ).limit( 2 ).offset( 2 ).order_by('id ASC').get_all('id,name');
	assert.deepEqual( offset.rows, [{ id: 3, name: 'George' }, { id: 4, name: 'Janet' }] , 'Offset '+ (++cnt) +' failed ' + JSON.stringify( offset, null, '  ' ) );

	offset = await SQL.query( 'test_users' ).offset( 3 ).order_by('id ASC').get_all('id,name');
	assert.deepEqual( offset.rows, [{ id: 4, name: 'Janet' }, { id: 5, name: 'Janet' }, { id: 6, name: 'Max' }, { id: 7, name: 'Janet' }] , 'Offset '+ (++cnt) +' failed ' + JSON.stringify( offset, null, '  ' ) );

}).timeout(100000);

it( 'Having', async() =>
{
	let cnt = 0;
	let having = await SQL.query( 'test_users' ).group_by( 'name' ).order_by('name ASC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
	assert.deepEqual( having.rows, [{ name: 'Janet', count: 3 },{ name: 'Max', count: 2 }] , 'Having '+ (++cnt) +' failed ' + JSON.stringify( having, null, '  ' ) );

	having = await SQL.query( 'test_users' ).order_by('name ASC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
	assert.deepEqual( having.rows, [{ name: 'Max', count: 7 }] , 'Having '+ (++cnt) +' failed ' + JSON.stringify( having, null, '  ' ) );

}).timeout(100000);

it( 'Select', async() =>
{
	let cnt = 0;
	let select = await SQL.query( 'test_users' ).where( 'created > :time OR name = :name ', { time: '( NOW() - INTERVAL 1 YEAR )', name: new Buffer( 'test', 'base64') }  ).order_by('name DESC').having( 'COUNT(*) > 1' ).get_all('name, COUNT(*) count');
	assert.deepEqual( select.rows, [{ name: 'Max', count: 7 }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

}).timeout(100000);

it( 'Errors', async() =>
{
	let SQLError = require( '../../lib/errors.js');
	let cnt = 0;
	let getError = new SQLError( 'UNKNOWN_ERROR_CODE' ).get();
	assert.ok( getError && getError.full === 'UNKNOWN_ERROR_CODE', 'Error '+ (++cnt) +' failed ' + JSON.stringify( getError, null, '  ' ) );

	getError = new SQLError().list();
	assert.ok( getError && Object.keys(getError).length, 'Error '+ (++cnt) +' failed ' + JSON.stringify( getError, null, '  ' ) );

}).timeout(100000);

it( 'Where as object', async() =>
{
	let cnt = 0;
	let select = await SQL.query( 'join_users' ).where( { name: 'John' }  ).group_by('name').get_all('name, COUNT(*) count');
	assert.deepEqual( select.rows, [{ name: 'John', count: 1 }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'test_users' ).where( { '!name': ['Max', 'Janet' ], surname: null }  ).get_all('name');
	assert.deepEqual( select.rows, [{ name: 'John' },{ name: 'George' }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'test_users' ).where( { 'name': ['George', 'Max' ], surname: null }  ).group_by('name').get_all('name');
	assert.deepEqual( select.rows, [{ name: 'George' },{ name: 'Max' }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'test_users' ).where( { '!name': null }  ).group_by('name').order_by('name ASC').get_all('name');
	assert.deepEqual( select.rows, [{ name: 'George' },{ name: 'Janet' },{ name: 'John' },{ name: 'Max' }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

}).timeout(100000);

it( 'CONCAT', async() =>
{
	let cnt = 0;
	let select = await SQL.query( 'join_users').get('id, CONCAT( id , :separator, name ) alias', { separator: '_' } );
	assert.deepEqual( select.rows, [{ id: 1, alias: '1_John' }] , 'Select '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

	select = await SQL.query( 'join_users').where('( id = 1 AND CONCAT( id , :separator, name ) = :string )', { string: '1_John', separator : '_'  }).get('id, CONCAT( id , :separator, name ) alias, CONCAT( id , :separator, name ) alias2', { separator: '_' } );
	assert.deepEqual( select.rows, [{ id: 1, alias: '1_John', alias2: '1_John' }] , 'CONCAT '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

}).timeout(100000);

it( 'IF', async() =>
{
	let cnt = 0;
	let select = await SQL.query( 'join_users').where( 'CONCAT( id , :separator, name ) = :string', { string: '1_John', separator : '_'  } ).get('id, IF( id = 1, :text1, IF( id = 2, :text2, :text3 ) ) position', { text1: 'first', text2: 'second', text3: 'next', separator: '_' } );
	assert.deepEqual( select.rows, [{ id: 1, position: 'first' }] , 'IF '+ (++cnt) +' failed ' + JSON.stringify( select, null, '  ' ) );

}).timeout(100000);
