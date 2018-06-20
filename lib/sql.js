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
      this.connector = require( './connectors/mysql.js')( config.mysql, this.event.emit.bind( this.event ) );
    }
  }

  query( table, alias = undefined )
  {
    return new Query({ emit: this.event.emit.bind( this.event ), table, alias, connector: this.connector, tables: this.tables });
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
