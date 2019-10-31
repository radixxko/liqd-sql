'use strict';

const TimedPromise = require('liqd-timed-promise');
const SQLError = require( './errors.js');
const MAX_TIMEOUT_MS = 180000;

if( !Array.prototype.values )
{
	Object.defineProperty( Array.prototype, 'values',
	{
		value: Array.prototype[Symbol.iterator],
		configurable: false,
		writable: false
	});
}

module.exports = class Database
{
	constructor( database )
	{
		this.query = { table: database.table, table_alias: '', options: [], sql_table: null, database: database.database, prefix: database.prefix, suffix: database.suffix };
		this.connector = database.connector;
		this.tables = database.tables;

		if( database.alias ){ this.query.alias = database.alias; }
	}

	schema()
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let describe = await this.connector.database( this.query.database );

			if( describe.ok )
			{
				resolve( { ok: true, schema: describe.tables });
			}
			else { resolve( describe ); }
		});
	}

	create_query( tables, user_options = {} )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( this.query.database && typeof this.query.database === 'string' )
			{
				if( tables && Array.isArray( tables ) )
				{
					let multi_tables = {};
					for( let i = 0; i < tables.length; i++ )
					{
						if( typeof tables[i] === 'object' && !Array.isArray( tables[i] ) )
						{
							multi_tables = Object.assign( multi_tables, tables[i] );
						}
					}

					if( Object.values( multi_tables ).length ){ tables = multi_tables; }
				}

				if( tables && typeof tables === 'object' && !Array.isArray( tables ) )
				{
					let options = {
						result_type  : ( user_options.result_type && user_options.result_type === 'array' ? 'array' : 'string' ),
						default_rows : ( user_options.default_rows && typeof user_options.default_rows === 'object' && !Array.isArray( user_options.default_rows ) ? user_options.default_rows : {} ),
						drop_table   : ( user_options.drop_table ),
					};

					resolve({ ok: true, create: await this.connector.create_database( this.query.database, tables, options )});
				}
				else{ resolve({ ok: false, error: 'missing_schema' }); }
			}
			else{ resolve({ ok: false, error: 'empty_name' }); }
		});
	}

	create( tables, user_options = {} )//experimental, some protections add
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let start_time = process.hrtime();
			user_options['result_type'] = 'array';

			let queries = await this.create_query( tables, user_options )
			let create_result = { ok: true };
			if( queries.ok )
			{
				for( let i = 0; i < queries.create.length; i++ )
				{
					let executed_query = await this.connector.execute_query( queries.create[i], ( result ) =>
					{
						return result;
					})
					.timeout( remaining_ms )
					.catch( e => reject( e ));

					if( !executed_query.ok ){ create_result = executed_query; break; }
				}

				let elapsed_time = process.hrtime(start_time);
				let end_time = elapsed_time[0] * 1000 + elapsed_time[1] / 1000000;

				resolve( create_result );
			}
			else{ resolve( queries ); }
		});
	}

	drop_query( options = {} )
	{
		return { ok: true, query: this.connector.drop_database( this.query.database, options )};
	}

	drop( options = {} )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let start_time = process.hrtime();
			let drop = this.drop_query( options );

			if( drop.ok )
			{
				this.connector.execute_query( drop.query, ( result ) =>
				{
					let elapsed_time = process.hrtime(start_time);
					let end_time = elapsed_time[0] * 1000 + elapsed_time[1] / 1000000;

					resolve({ ok: result.ok });
				})
				.timeout( remaining_ms )
				.catch( e => reject( e ));
			}
			else{ resolve( drop ); }
		});
	}

	modify_query( tables, user_options = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( tables && Array.isArray( tables ) )
			{
				let multi_tables = {};
				for( let i = 0; i < tables.length; i++ )
				{
					if( typeof tables[i] === 'object' && !Array.isArray( tables[i] ) )
					{
						multi_tables = Object.assign( multi_tables, tables[i] );
					}
				}

				tables = multi_tables;
			}

			if( tables && typeof tables === 'object' && Object.values( tables ).length )
			{
				let error = [];

				let options = {
					result_type : 'string',
					default_rows   : {},
					drop_table    : false,
				};

				if( user_options && user_options.hasOwnProperty( 'result_type' ) )
				{
					if( [ 'string', 'array' ].includes( user_options.result_type ) ){ options.result_type = user_options.result_type; }
					else { error.push( 'result_type' ); }
				}

				if( user_options && user_options.hasOwnProperty( 'default_rows' ) )
				{
					if( typeof user_options.default_rows === 'object' && !Array.isArray( user_options.default_rows ) ){ options.default_rows = user_options.default_rows; }
					else { error.push( 'default_rows' ); }
				}

				if( user_options && user_options.hasOwnProperty( 'drop_table' ) )
				{
					options[ 'drop_table' ] = ( user_options.drop_table );
				}

				if( !error.length )
				{
					resolve( { ok: true, create: await this.connector.modify_database( tables, options )});
				}
				else { resolve({ ok: false, error: error }); }
			}
			else { resolve({ ok: false, error: [ 'tables' ] }); }
		});
	}
};
