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

module.exports = class Table
{
	constructor( table )
	{
		this.query = { table: table.table, table_alias: '', options: [], sql_table: null, database: table.database, prefix: table.prefix, suffix: table.suffix };
		this.connector = table.connector;
		this.tables = table.tables;

		if( table.alias ){ this.query.alias = table.alias; }
	}

	columns( options = [] )
	{
		if( this.tables.hasOwnProperty( this.query.table ) && this.tables[ this.query.table ].columns )
		{
			return Object.keys( this.tables[ this.query.table ].columns );
		}
		else
		{
			let set_start_time = process.hrtime();

			return new TimedPromise( async( resolve, reject, remaining_ms ) =>
			{
				let schema = await this.connector.describe_table( this.query.table );
				resolve( ( schema.ok ? Object.keys( schema.table.columns ) : [] ));
			});
		}
	}

	create_query( options = [] )
	{
		return this.connector.create_table_query( this.query.table, this.query.alias, this.query.database, options );
	}

	create( options = [] )
	{
		let set_start_time = process.hrtime();

		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !remaining_ms ){ remaining_ms = MAX_TIMEOUT_MS; }

			this.connector.execute_query( this.connector.create_table_query( this.query.table, this.query.alias, this.query.database, options ), ( result ) =>
			{
				let set_elapsed_time = process.hrtime(set_start_time);
				result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

				resolve( result );
			})
			.timeout( remaining_ms )
			.catch( e => reject( e ));
		});
	}

	truncate_query()
	{
		this.query.operation = 'truncate';
		return this.connector.db_connector.build( this.query );
	}

	truncate()
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !remaining_ms ){ remaining_ms = MAX_TIMEOUT_MS; }

			let set_start_time = process.hrtime();
			if( this.query.table )
			{
				this.query.operation = 'truncate';

				this.connector.execute_query( this.query, ( result ) =>
				{
					let set_elapsed_time = process.hrtime(set_start_time);
					result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;
					resolve( result );
				})
					.timeout( remaining_ms )
					.catch( e => reject( e )); //TODO
			}
			else{ resolve( { ok : false, error: { code: 'UNDEFINED_TABLE' }, connector_error: new SQLError({ code: 'UNDEFINED_TABLE' }).get()}); }
		});
	}

	drop_query( options = [] )
	{
		return this.connector.drop_table_query( this.query.table, this.query.database, options );
	}

	drop( options = [] )
	{
		let set_start_time = process.hrtime();

		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !remaining_ms ){ remaining_ms = MAX_TIMEOUT_MS; }

			this.connector.execute_query( this.connector.drop_table_query( this.query.table, this.query.database, options ), ( result ) =>
			{
				let set_elapsed_time = process.hrtime(set_start_time);
				result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

				resolve( result );
			})
			.timeout( remaining_ms )
			.catch( e => reject( e ));
		});
	}

	duplicate( name, data = false )
	{
		let set_start_time = process.hrtime();

		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			resolve( await this.connector.duplicate_table( this.query.table, name, data ) );
		});
	}

	schema( options = [] )
	{
		let set_start_time = process.hrtime();

		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			resolve( await this.connector.describe_table( this.query.table ) );
		});
	}

	info( options = [] )
	{

	}
}
