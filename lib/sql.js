'use strict';

const Event = require('liqd-event');
const Query = require('./query');
const Connector = require('./connector');

module.exports = class SQL
{
	constructor( config )
	{
		if( config && Object.keys(config).length )
		{
			if( !config.hasOwnProperty( 'connector' ) )
			{
				let connectors = [];
				Object.keys( config ).forEach( name => { if( [ 'mysql', 'oracle', 'mssql', 'postgre' ].includes( name ) ){ connectors.push(name); } });

				if( connectors.length ){ config['connector'] = connectors[0]; }
			}

			this.tables = config.tables;
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
				this.db_connector = require( './connectors/postgre.js')( config.postgre, this.event.emit.bind( this.event ) );
			}
			else if( config.connector === 'mssql' && config.mssql )
			{
				this.db_connector = require( './connectors/mssql.js')( config.mssql, this.event.emit.bind( this.event ) );
			}
			else { throw new Error('missing database config'); }

			this.connector = new Connector( { db_connector: this.db_connector, emit: this.event.emit.bind( this.event ), on: this.event.on.bind( this.event ), tables: this.tables } );
		}
		else { throw new Error('missing config'); }
	}

	query( table, alias = undefined )
	{
		return new Query({ table, alias, connector: this.connector, tables: this.tables });
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
