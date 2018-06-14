'use strict';

const Event = require('liqd-event');
const Query = require('./query');

module.exports = class SQL
{
	constructor( config )
	{
		this.tables = config.tables;
		this.connector = null;
		this.event = new Event();

		if( config.mysql )
    {
      this.connector = require( './connectors/mysql.js')( config.mysql );
    }
	}

	query( table, alias = undefined )
	{
		return new Query({ emit: this.event.emit, table, alias, connector: this.connector, tables: this.tables });
	}

	on( event, handler )
	{
		this.event.on( event, handler );
	}

	off( event, handler )
	{
		this.event.off( event, handler );
	}
}
