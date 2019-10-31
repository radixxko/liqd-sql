'use strict';

const Event = require('liqd-event');
const Query = require('./query');
const Table = require('./table');
const Database = require('./database');
const Connector = require('./connector');

module.exports = class SQL
{
	constructor( config )
	{
		if( config && Object.keys(config).length )
		{
			if( !config.connector ){ config.connector = [ 'mysql', 'mssql', 'postgre', 'oracle' ].find( c => config[c] ); }

			this.tables = {};
			if( config.tables )
			{
				if( Array.isArray( config.tables ) )
				{
					for( let i = 0; i < config.tables.length; i++ )
					{
						if( typeof config.tables[i] === 'object' && !Array.isArray( config.tables[i] ) )
						{
							this.tables = Object.assign( this.tables, config.tables[i] );
						}
					}
				}
				else { this.tables = config.tables; }
			}

			this.event = new Event();

			if( config.connector === 'mysql' && config.mysql )
			{
				this.db_connector = require( './connectors/mysql.js')( config.mysql, this.event.emit.bind( this.event ) );
			}
			else if( config.connector === 'oracle' && config.oracle )
			{
				this.db_connector = require( './connectors/oracle.js')( config.oracle, this.event.emit.bind( this.event ) );
			}
			else if( config.connector === 'postgre' && config.postgre )
			{
				//this.db_connector = require( './connectors/postgre.js')( config.postgre, this.event.emit.bind( this.event ) ); //TODO
			}
			else if( config.connector === 'mssql' && config.mssql )
			{
				this.db_connector = require( './connectors/mssql.js')( config.mssql, this.event.emit.bind( this.event ) );
			}
			else { throw new Error('missing database config'); }

			this.database_name = config.database;
			this.prefix = config.prefix;
			this.suffix = config.suffix;

			this.connector = new Connector( { db_connector: this.db_connector, emit: this.event.emit.bind( this.event ), on: this.event.on.bind( this.event ), tables: this.tables } );
		}
		else { throw new Error('missing config'); }
	}

	query( table, alias = undefined )
	{
		return new Query({ table, alias, connector: this.connector, tables: this.tables, database: this.database_name, prefix: this.prefix });
	}

	table( table, alias = undefined )
	{
		return new Table({ table, alias, connector: this.connector, tables: this.tables, database: this.database_name, prefix: this.prefix });
	}

	database( table, alias = undefined )
	{
		return new Database({ table: null, alias, connector: this.connector, tables: this.tables, database: table, prefix: this.prefix });
	}

	on( event, handler )
	{
		this.event.on( event, handler );
	}

	off( event, handler )
	{
		this.event.off( event, handler );
	}

	get connected()
	{
		return this.connector.db_connector.connected;
	}
}
