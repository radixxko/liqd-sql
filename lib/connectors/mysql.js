'use strict';

const Abstract_Connector = require( './abstract.js');
const TimedPromise = require('liqd-timed-promise');
const SQLError = require( '../errors.js');

let reconnectTime = 15000, reconnectTimeRemaining = 15000;
let emptied = false;
let connectionCheck = false, disconnected = false, my_container = [];
let connectionLostErrors = ['PROTOCOL_CONNECTION_LOST', 'ECONNREFUSED', 'ECONNRESET'];

function getMiliseconds()
{
  return (new Date()).getTime();
}

module.exports = function( config, emit )
{
  return new( function()
  {
    const MYSQL_Connector = this;
    this.emit = emit;

    var my_sql = null,
        my_connections = null,
        my_connected = false,
        default_charset = 'utf8_general_ci';

    function connect()
    {
      var options = JSON.parse(JSON.stringify(config));

      if( typeof options.charset == 'undefined' ) {   options.charset = 'utf8mb4'; }
      if( typeof options.timezone == 'undefined' ){   options.timezone = 'utc';    }
      if( typeof options.connectionLimit == 'undefined' ){ options.connectionLimit = 10; }
      if( options.reconnectTime && !isNaN( options.reconnectTime ) ){ reconnectTime = reconnectTimeRemaining = options.reconnectTime; }

      if( typeof options.default_charset != 'undefined' ){ default_charset = options.default_charset; }

      options.dateStrings        = 'date';
      options.supportBigNumbers  = true;

      my_sql = require( 'mysql');
      my_connections = my_sql.createPool( options );

      checkConnect( true );
    }
    connect();

    function disconnect()
    {
      if( reconnectTimeRemaining === reconnectTime )
      {
        --reconnectTimeRemaining;
        setTimeout( async () =>
        {
          let disconnectTime = getMiliseconds() + reconnectTime;
          if( !my_connected )
          {
            while( reconnectTimeRemaining > 1 && !my_connected )
            {
              reconnectTimeRemaining = disconnectTime - getMiliseconds();
            }

            disconnected = true;
          }
        }, 1);
      }
    }

    function checkConnect( oneTime = false )
    {
      //if( !disconnected )
      {
        let isConnected = ( my_connected );



        if( my_connections )
        {
          my_connections.query('SELECT \'connected\' FROM DUAL', async ( err, rows ) =>
          {
            if( err && err.code &&  connectionLostErrors.includes( err.code ) )
            {
              my_connected = false;

              //if( reconnectTimeRemaining === 3000 ){  disconnect() }

              if( ( connectionCheck && !oneTime ) ){ connect(); }
              else if( !connectionCheck ){ setTimeout( connect, 1000 ); }
            }
            else if( !err )
            {
              my_connected = true;
            }

            if( my_connected && my_container.length && !emptied ) { emptyContainer(); }
            if( !oneTime  ){ setTimeout( checkConnect, 1000 ); }
          });
        }
        else {
          my_connected = false;
          if( ( connectionCheck && !oneTime ) ){ connect(); }
          else if( !connectionCheck ){ setTimeout( connect, 1000 ); }
        }
      }
    }

    async function connection_state()
    {
      if( !connectionCheck ) { connectionCheck = true; checkConnect(); }

      return { connected: my_connected };
    }

    connection_state();

    async function emptyContainer()
    {
      if( my_container.length && !emptied )
      {
        emptied = true;
        let prepare_promises = [];

        do {
          let limit_conncetion = Math.min( 10, my_container.length );

          for( let i = 0; i < limit_conncetion; i++ )
          {
            prepare_promises.push( MYSQL_Connector[my_container[i].type]( my_container[i].query, my_container[i].callback ) );
          }

          my_container.splice( 0, limit_conncetion );
        }
        while( my_connected && my_container.length );

        if( emptied ) { emptied = false };
      }
    }

    function aggregate_columns( columns, aggregator = 'MAX' )
    {
      let columnRE = /^((`[^`]+`\.)*(`[^`]+`))$/m;
      let columnsRE = /((AVG|BIT_AND|BIT_OR|BIT_XOR|CHECKSUM_AGG|COUNT|COUNT_BIG|GROUP_CONCAT|GROUPING|GROUPING_ID|JSON_ARRAYAGG|JSON_OBJECTAGG|MAX|MIN|STD|STDDEV|STDDEV_POP|STDDEV_SAMP|STRING_AGG|SUM|VAR|VARP|VAR_POP|VAR_SAMP|VARIANCE)\s*\(|([\)0-9'"`]{0,1}))\s*((`[^`]+`\.)*`[^`]+`)/gm, column, lastOffset = null;
      let stringsRE = /('[^']*'|"[^"]*")/gm;

      return columns.replace( columnsRE, ( match, _, aggregated, alias, column, __, offset ) =>
      {
        let aggregate = !( aggregated || alias || lastOffset === offset ); lastOffset = offset + match.length;

        if( aggregate )
        {
          let tail = columns.substr( offset + match.length );
          match = match.substr( 0, match.length - column.length ) + aggregator + '(' + column + ')';

          if( !tail || /^\s*,/.test(tail) )
          {
            let parentheses = columns.substr( offset + match.length ).replace( stringsRE, '' ).replace(/[^\(\)]/g, '');
            if( parentheses.replace(/\(/g, '').length === parentheses.length / 2 )
            {
              match += ' ' + column.replace( columnRE, '$3' );
            }
          }
        }

        return match;
      });
    }

    this.escape_column = function( column )
    {
      if(column)
      {
        return '`' + column + '`';
      }
      else return '';
    };

    this.escape_value = function( value )
    {
      if( typeof value === 'string' )
      {
        value = value.replace(/\\/g, '\\\\');
        if( value.indexOf( 'NOW()' ) !== -1 ) //todo solve this
        {
          return value;
        }
        else
        {
          if (value.indexOf('&__escaped__:') === 0) {
          return value.substr('&__escaped__:'.length);
        }

        return '\'' + value.replace(/'/g, '\'\'') + '\'';
        }
      }
      else if( typeof value === 'number' )
      {
        return value;
      }
      else if( !value )
      {
        return null; // TODO bud NULL, alebo prazdny string, vyskusat
      }
        else if( value instanceof Buffer )
      {
        return 'X\'' + value.toString('hex') + '\'';
      }
        else
      {
        return '\'' + value.toString().replace(/'/g, '\'\'') + '\'';
      }
    };

    this.get_tables_columns = function( columns )
    {
      return Abstract_Connector.get_tables_columns( columns );
    }

    this.build = function( query )
    {
      let querystring = '';

      if( query.hasOwnProperty( 'union' ) )  //todo if tables is defined, then can check if is column count equal
      {
        let used_columns = [];

        if( query.columns ) { used_columns = Abstract_Connector.get_used_columns( query.columns.columns, query.alias, used_columns ); }
        if( query.where ) { for( let i = 0; i < query.where.length; ++i ){ used_columns = Abstract_Connector.get_used_columns( query.where[i].condition, query.alias, used_columns ); } }
        if( query.join )  { for( let i = 0; i < query.join.length; ++i ) { used_columns = Abstract_Connector.get_used_columns( query.join[i].condition, query.alias, used_columns ); } }
        if( query.order ) { used_columns = Abstract_Connector.get_used_columns( query.order.condition, query.alias, used_columns ); }
        if( query.group_by ) { used_columns = Abstract_Connector.get_used_columns( query.group_by.condition, query.alias, used_columns ); }

        let ia_uses = 0;
        let unions = [], inner_alias = 'ua_' ;

        for( let union of query.union )
        {
          if( Array.isArray( union ) )
          {
            let select_dual = [];
            for (var i = 0; i < union.length; i++)
            {
              if( typeof union[i] === 'object' )
              {
                let dual_data = [];

                if( used_columns.length )
                {
                  for( let p = 0; p < used_columns.length; p++ )
                  {
                    dual_data.push( ( union[i].hasOwnProperty( used_columns[p] ) ? MYSQL_Connector.escape_value( union[i][ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MYSQL_Connector.escape_column)  );
                  }
                }
                else
                {
                  for( let column in union[i] )
                  {
                    if( union[i].hasOwnProperty( column ) ){ dual_data.push( MYSQL_Connector.escape_value( union[i][ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MYSQL_Connector.escape_column)  ); }
                  }
                }

                select_dual.push( 'SELECT ' + dual_data.join( ', ' )  );
              }
            }

            unions.push( 'SELECT * FROM ( ' + select_dual.join( ' UNION ALL ' ) + ' ) '+inner_alias+ (ia_uses++) );
          }
          else if( typeof union === 'object' && union.table )
          {
            if( !union.columns ){ union.columns = { columns: '*' , data: null }; }

            union.operation = 'select';
            let subquery = this.build( union );
            unions.push( 'SELECT '+ ( used_columns.length ? used_columns.join(', ') : '*' ) +' FROM ( ' + subquery + ' ) '+inner_alias+ (ia_uses++) );
          }
          else if( typeof union === 'object' )
          {
            let dual_data = [];
            if( used_columns.length )
            {
              for( let p = 0; p < used_columns.length; p++ )
              {
                dual_data.push( ( union.hasOwnProperty( used_columns[p] ) ? MYSQL_Connector.escape_value( union[ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MYSQL_Connector.escape_column)  );
              }
            }
            else
            {
              for( let column in union )
              {
                if( union.hasOwnProperty( column ) ){ dual_data.push( MYSQL_Connector.escape_value( union[ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MYSQL_Connector.escape_column)  ); }
              }
            }

            unions.push( 'SELECT ' + dual_data.join( ', ' )  );
          }
          else if( typeof union === 'string' )
          {
            unions.push( union );
          }
        }

        if( query.operation === 'union'  )
        {
          querystring = unions.join( ' UNION ' );
        }
        else { query.table = '( '+ unions.join( ' UNION ' ) +' ) ' + ( query.alias ? query.alias : 'ua_d'+ Math.ceil( Math.random() * 10000000 ) ); };
      }

      if( query.operation === 'select' )
      {
        let escaped_columns = Abstract_Connector.escape_columns( Abstract_Connector.expand_values( query.columns.columns, query.columns.data, MYSQL_Connector.escape_value ), MYSQL_Connector.escape_column );

        if( query.order && ( query.group_by || query.having ) )
        {
          escaped_columns = aggregate_columns( escaped_columns, 'MAX' );
        }

        querystring += 'SELECT ' + escaped_columns + ( ( query.table && query.table != 'TEMPORARY' ) ? ' FROM ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column ) : '' );
      }
      else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
      {
        querystring += 'INSERT '+( query.options && query.options.indexOf('ignore') !== -1  ? 'IGNORE' : '' )+' INTO ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column ) + ' (' + Abstract_Connector.expand( query.columns, MYSQL_Connector.escape_column ) + ') VALUES ';

        for( let i = 0; i < query.data.length; ++i )
        {
          querystring += ( i === 0 ? '' : ',' ) + '(';

          for( let j = 0; j < query.columns.length; ++j )
          {
            querystring += ( j === 0 ? '' : ',' ) + ( typeof query.data[i][query.columns[j]] !== 'undefined' ? MYSQL_Connector.escape_value(query.data[i][query.columns[j]]) : null );
          }

          querystring += ')';
        }
      }
      else if( query.operation === 'update' )
      {
        querystring += 'UPDATE ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column );
      }
      else if( query.operation === 'delete' )
      {
        querystring += 'DELETE FROM ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column );
      }

      if( query.join )
      {
        for( let i = 0; i < query.join.length; ++i )
        {
          if( typeof query.join[i].table === 'object' && query.join[i].table )
          {
            if( !query.join[i].table.columns ){ query.join[i].table.columns = { columns: '*' , data: null }; }

            query.join[i].table.operation = 'select';
            let subquery = this.build( query.join[i].table );
            querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN (' + subquery + ') ' + query.join[i].table.alias + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ), MYSQL_Connector.escape_column );
          }
          else
          {
            querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ), MYSQL_Connector.escape_column );
          }
        }
      }

      if( query.set )
      {
        let set = '';

        if( query.operation === 'update' && query.hasOwnProperty( 'update_with_where' ) && query.update_with_where )
        {
          let columns = query.set.columns;
          for( let i = 0; i < columns.length; ++i )
          {
            if( typeof query.data[0][columns[i]] !== 'undefined' )
            {
              set += ( i ? ', ' : '' ) + MYSQL_Connector.escape_column(columns[i]) + ' = ' + MYSQL_Connector.escape_value(query.data[0][columns[i]]);
            }
          }

          //set = toSet
        }
        else if( query.data && Array.isArray(query.data) )
        {
          let columns = query.set.columns;
          let groups_indexes = query.set.indexes;

          for( let i = 0; i < columns.length; ++i )
          {
            if( columns[i].substr(0,2) == '__' && columns[i].substr(-2) == '__' ){ continue; }
            set += ( i ? ', ' : '' ) + MYSQL_Connector.escape_column(columns[i]) + ' = CASE';

            for( let j = 0; j < query.data.length; ++j )
            {
              if( typeof query.data[j][columns[i]] !== 'undefined' )
              {
                set += ' WHEN ';
                let indexes = query.data[j].__indexes__;

                if( !indexes )
                {
                  indexes = groups_indexes[0];

                  if( groups_indexes.length > 1 )
                  {
                    for( let index of groups_indexes )
                    {
                      let missing = false;

                      for( let k = 0; k < index.length; k++ )
                      {
                        if( !query.data[j].hasOwnProperty( index[k] ) )
                        {
                          missing = true;
                          break;
                        }
                      }

                      if( !missing ){ indexes = index; break; }
                    }
                  }
                }

                for( let k = 0; k < indexes.length; ++k )
                {
                  set += ( k ? ' AND ' : '' ) + MYSQL_Connector.escape_column(indexes[k]) + ' = ' + MYSQL_Connector.escape_value(query.data[j][indexes[k]]);
                }

                set += ' THEN ' + MYSQL_Connector.escape_value(query.data[j][columns[i]]);
              }
            }

            set += ' ELSE ' + MYSQL_Connector.escape_column(columns[i]) + ' END';
          }
        }
        else
        {
          set = Abstract_Connector.escape_columns(query.set, MYSQL_Connector.escape_column);

          if(query.data !== null)
          {
            set = Abstract_Connector.expand_values(set, query.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column);
          }
        }

        querystring += ' SET ' + set;
      }

      if( query.where )
      {
        let where = '';

        for( let i = 0; i < query.where.length; ++i )
        {
          let condition = Abstract_Connector.escape_columns( query.where[i].condition, MYSQL_Connector.escape_column );

          if( typeof query.where[i].data !== 'undefined' )
          {
            condition = Abstract_Connector.expand_values( condition, query.where[i].data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column );
          }

          if( i === 0 )
          {
            where = condition;
          }
          else
          {
            if( i == 1 )
            {
              where = '( ' + where + ' )';
            }

            where += ' AND ( ' + condition + ' )';
          }
        }

        querystring += ' WHERE ' + where;
      }

      if( query.group_by )
      {
        let condition = Abstract_Connector.escape_columns( query.group_by.condition, MYSQL_Connector.escape_column );

        querystring += ' GROUP BY ' + ( query.group_by.data ? Abstract_Connector.expand_values( condition, query.group_by.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ) : condition );
      }

      if( query.having )
      {
        let condition = Abstract_Connector.escape_columns( query.having.condition, MYSQL_Connector.escape_column );

        querystring += ' HAVING ' + ( query.having.data ? Abstract_Connector.expand_values( condition, query.having.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ) : condition );
      }

      if( query.order )
      {
        let condition = Abstract_Connector.escape_columns( query.order.condition, MYSQL_Connector.escape_column );

        querystring += ' ORDER BY ' + ( query.order.data ? Abstract_Connector.expand_values( condition, query.order.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ) : condition );
      }

      if( query.limit )
      {
        querystring += ' LIMIT ' + query.limit;
      }

      if( query.offset )
      {
        if( !query.limit ) { querystring += ' LIMIT 18446744073709551615'; }
        querystring += ' OFFSET ' + query.offset;
      }

      return querystring;
    };
/*
    this.query = function( query )
    {
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        my_connections.query( query, ( err ) =>
        {
          resolve( { ok: !Boolean(err) } );
        });
      });
    };
*/
    this.select = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        const start_time = process.hrtime();

        my_connections.query( query, ( err, rows ) =>
        {
          const elapsed_time = process.hrtime(start_time);
          let result =
          {
            ok            : true,
            error          : null,
            affected_rows  : 0,
            changed_rows  : 0,
            inserted_id    : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids    : [],
            row            : null,
            rows          : [],
            sql_time      : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000,
            query          : query
          };

          if( err )
          {
            if( connectionLostErrors.includes( err.code ) )
            {
              my_container.push({ query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'select' });
            }
            else
            {
              result.ok = false;
              result.error = new SQLError( err ).get();

              this.emit( 'query', result );
              resolve( result );
            }
          }
          else
          {
            for( let i = 0; i < rows.length; ++i )
            {
              result.rows.push( rows[i] );
            }

            if( result.rows.length )
            {
              result.row = result.rows[0];
            }

            result.affected_rows = rows['affectedRows'] || result.rows.length;

            this.emit( 'query', result );
            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      }).timeout( reconnectTimeRemaining, 'disconnected' ).catch( error => { return { status: 'error', error: error }; });
    };

    this.update = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        const start_time = process.hrtime();

        my_connections.query(query, ( err, rows ) =>
        {
          const elapsed_time = process.hrtime(start_time);
          let result =
          {
            ok            : true,
            error          : null,
            affected_rows  : 0,
            changed_rows  : 0,
            inserted_id    : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids    : [],
            row            : null,
            rows          : [],
            sql_time      : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000,
            query          : query
          };

          if( err )
          {
            if( connectionLostErrors.includes( err.code ) )
            {
              my_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'update' });
            }
            else
            {
              result.ok = false;
              result.error = new SQLError( err ).get();

              this.emit( 'query', result );
              resolve( result );
            }
          }
          else
          {
            result.affected_rows = rows['affectedRows'];
            result.changed_rows = rows['changedRows'];

            this.emit( 'query', result );
            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      }).timeout( reconnectTimeRemaining, 'disconnected' ).catch( error => { return { status: 'error', error: error }; });
    };

    this.insert = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        const start_time = process.hrtime();

        my_connections.query(query, ( err, rows ) =>
        {
          const elapsed_time = process.hrtime(start_time);
          let result =
          {
            ok            : true,
            error          : null,
            affected_rows  : 0,
            changed_rows  : 0,
            inserted_id    : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids    : [],
            row            : null,
            rows          : [],
            sql_time      : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000,
            query          : query
          };

          if( err )
          {
            if( connectionLostErrors.includes( err.code ) )
            {
              my_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'insert' });
            }
            else
            {
              result.ok = false;
              result.error = new SQLError( err ).get();

              this.emit( 'query', result );
              resolve( result );
            }
          }
          else
          {
            result.changed_rows = result.affected_rows = rows['affectedRows'];

            if( rows['affectedRows'] && rows.insertId )
            {
              for( let i = 0; i < rows['affectedRows']; ++i )
              {
                result.inserted_ids.push( rows.insertId + i );
                result.changed_ids.push( rows.insertId + i );
              }

              result.inserted_id = result.changed_id = result.inserted_ids[0];
            }

            this.emit( 'query', result );
            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      }).timeout( reconnectTimeRemaining, 'disconnected' ).catch( error => { return { status: 'error', error: error }; });
    };

    this.delete = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        const start_time = process.hrtime();

        my_connections.query( query, ( err, rows ) =>
        {
          const elapsed_time = process.hrtime(start_time);

          let result =
          {
            ok            : true,
            error          : null,
            affected_rows  : 0,
            changed_rows  : 0,
            inserted_id    : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids    : [],
            row            : null,
            rows          : [],
            sql_time      : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000,
            query          : query
          };

          if( err )
          {
            if( connectionLostErrors.includes( err.code ) )
            {
              my_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'delete' });
            }
            else
            {
              result.ok = false;
              result.error = new SQLError( err ).get();

              this.emit( 'query', result );
              resolve( result );
            }
          }
          else
          {
            result.affected_rows = rows['affectedRows'];
            result.changed_rows = rows['changedRows'];

            this.emit( 'query', result );
            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      }).timeout( reconnectTimeRemaining, 'disconnected' ).catch( error => { return { status: 'error', error: error }; });
    };

    this.show_table_index = function( table )
    {
      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        this.select( 'SHOW INDEX FROM ' + this.escape_column( table )).then( ( tableIndex ) =>
        {
          let indexes = { primary: {}, unique: {}, index: {} };

          if( tableIndex.ok )
          {
            if( tableIndex.rows && tableIndex.rows.length > 0 )
            {
              let indexData = { primary: {}, unique: {}, index: {} };

              for( let j = 0; j < tableIndex.rows.length; j++ )
              {
                let indexType = ( tableIndex.rows[j]['Key_name']  === 'PRIMARY' ? 'primary' : ( tableIndex.rows[j]['Non_unique'] === 0 ? 'unique' : 'index' ) );

                if( !indexData[ indexType ].hasOwnProperty( tableIndex.rows[j]['Key_name'] ) )
                {
                  indexData[ indexType ][ tableIndex.rows[j]['Key_name'] ] = [];
                }

                indexData[ indexType ][ tableIndex.rows[j]['Key_name'] ].push( tableIndex.rows[j]['Column_name'] );
              }

              for( let index in indexData )
              {
                if( indexData[ index ] )
                {
                  for( let index_part in indexData[ index ] )
                  {
                    if( indexData[ index ].hasOwnProperty( index_part ) )
                    {
                      indexes[ index ][ index_part ] = indexData[ index ][ index_part ].join( ',' );
                    }
                  }
                }
              }

              indexes = {
                primary : ( indexes.primary.PRIMARY ? indexes.primary.PRIMARY  : '' ),
                unique  : Object.values( indexes.unique),
                index   : Object.values( indexes.index)
              };
            }
          }

          resolve({ ok: true, indexes: indexes });
        });
      });
    };

    this.describe_columns = function( table )
    {
      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        this.select( 'SHOW FULL COLUMNS FROM ' + this.escape_column( table )).then( ( columnsData ) =>
        {
          if( columnsData.ok && columnsData.rows.length )
          {
            let columns = {};

            for( let k = 0; k < columnsData.rows.length; k++ )
            {
              let columnName = columnsData.rows[ k ]['Field'];

              columns[ columnName ] = {};
              columns[ columnName ].type = columnsData.rows[ k ]['Type'].split(/[\s,:(]+/)[0].toUpperCase();

              let match = columnsData.rows[ k ]['Type'].match(/\((.*?)\)/)
              if( match ){ columns[ columnName ].type += ':' + match[1].replace(/'/g, ''); }

              if( columnsData.rows[ k ]['Type'].indexOf( 'unsigned' ) !== - 1) { columns[ columnName ].unsigned = true; }

              if( columnsData.rows[ k ]['Null'] === 'YES' ) { columns[ columnName ].null = true; }

              if( columnsData.rows[ k ]['Default'] || columnsData.rows[ k ]['Default'] === '' )
              {
                columns[ columnName ].default = columnsData.rows[ k ]['Default'];
              }
              else if( columnsData.rows[ k ]['Null'] === 'YES' && columnsData.rows[ k ]['Default'] === null )
              {
                columns[ columnName ].default = 'NULL';
              }

              if( columnsData.rows[ k ]['Extra'] === 'on update CURRENT_TIMESTAMP' )
              {
                columns[ columnName ].update = 'CURRENT_TIMESTAMP';
              }
              else if( columnsData.rows[ k ]['Extra'] === 'auto_increment' )
              {
                columns[ columnName ].increment = true;
              }

              if( columnsData.rows[ k ]['Collation'] !== default_charset )
              {
                columns[ columnName ].multibyte = columnsData.rows[ k ]['Collation'];
              }
            }

            resolve({ ok: true, columns: columns });
          }
          else { resolve({ ok: false, error: columnsData.error }); }
        });
      });
    };

    this.create_table = function( table, name )
    {
      let columns = [], indexes = [];
      let querystring = 'CREATE TABLE ' + this.escape_column( name );

      for( let column in table.columns )
      {
        if( table.columns.hasOwnProperty( column ) )
        {
          let columnData =  ' ' + this.escape_column( column ) + ' ' + this.create_column( table.columns[ column ] );
          columns.push( columnData );
        }
      }

      for( let type in table.indexes )
      {
        if( table.indexes.hasOwnProperty(type) && table.indexes[type] && table.indexes[type].length > 0 )
        {
          let keys = ( typeof table.indexes[type] === 'string' ? [ table.indexes[type] ] : table.indexes[type] );

          for( let i = 0; i < keys.length; ++i )
          {
            let alterTableIndexes = this.create_index( keys[i], type, table.columns );

            if( alterTableIndexes )
            {
              indexes.push( alterTableIndexes );
            }
          }
        }
      }

      querystring += ' (' + columns.concat( indexes ).join(',') + ' ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_general_ci;';

      return querystring;
    }

    this.drop_table = function( table )
    {
      let querystring = 'DROP TABLE IF EXISTS ' + this.escape_column( table );
      return querystring;
    }

    this.create_column = function( columnData )
    {
      let column = '';

      if( columnData )
      {
        if( columnData['type'] )
        {
          let type = columnData['type'].split(/[\s,:]+/)[0];

          column += ' ' + type.toLowerCase();

          let size = columnData['type'].match(/:([0-9]+)/);

          if( type === 'DECIMAL' )
          {
            column += '('+columnData['type'].match(/:([0-9,]+)/)[1]+')';
          }
          else if( size )
          {
            column += '(' + size[1] + ')';
          }
          else if( type === 'INT' )
          {
            column += '(11)';
          }
          else if( type === 'BIGINT' )
          {
            column += '(20)';
          }
          else if( type === 'TINYINT' )
          {
            column += '(3)';
          }

          if( ['SET', 'ENUM'].indexOf( type ) > -1 )
          {
            column += '(' + Abstract_Connector.expand( columnData['type'].split(':')[1].trim().split(/\s*,\s*/), this.escape_value ) + ')';
          }

          if( ( [ 'INT', 'BIGINT', 'TINYINT' ].indexOf(type) !== -1 && columnData['type'].toUpperCase().indexOf(':UNSIGNED') !== -1 ) || columnData['unsigned']  )
          {
            column += ' unsigned';
          }

          if( ( [ 'VARCHAR' ].indexOf(type) !== -1 && columnData['type'].toLowerCase().indexOf('multibyte_bin') !== -1 ) || columnData['multibyte_bin'] )
          {
            column += ' CHARACTER SET utf8mb4 COLLATE utf8mb4_bin';
          }
          else if( ( [ 'VARCHAR' ].indexOf(type) !== -1 && columnData['type'].toLowerCase().indexOf('multibyte') !== -1 ) || columnData['multibyte'] )
          {
            column += ' CHARACTER SET utf8mb4 COLLATE utf8mb4_slovak_ci';
          }

          column += ( columnData['null'] ? ' NULL' : ' NOT NULL'  );

          if( typeof columnData['default'] !== 'undefined' )
          {
            if( type === 'DECIMAL' )
            {
              column += ' DEFAULT ' + ( ['CURRENT_TIMESTAMP', 'NULL'].indexOf(columnData['default']) === -1 ? this.escape_value( parseFloat( columnData['default'] ).toString() ) : parseFloat( columnData['default'] ) );
            }
            else
            {
              column += ' DEFAULT ' + ( ['CURRENT_TIMESTAMP', 'NULL'].indexOf(columnData['default']) === -1 ? this.escape_value( columnData['default'].toString() ) : columnData['default'] );
            }
          }

          if( columnData['update'] )
          {
            column += ' ON UPDATE ' + ( ['CURRENT_TIMESTAMP'].indexOf(columnData['update']) === -1 ? this.escape_value( columnData['update'] ) : columnData['update'] );
          }

          if( columnData['increment'] )
          {
            column += ' AUTO_INCREMENT';
          }
        }
      }

      return column;
    }

    this.create_index = function( index, type, columns, alter )
    {
      if( !alter ){ alter = false; }

      let cols = index.split(/\s*,\s*/);
      let sql =  ( alter ? 'ADD ' : ' ' ) + ' ' + ( type === 'primary' ? 'PRIMARY KEY ' : ( type === 'unique' ? ( alter ? 'UNIQUE INDEX ' : 'UNIQUE KEY ' ) : ( type === 'index' ? ( alter ? 'INDEX ' : 'KEY ' ) : '' ) ) ) + ( type !== 'primary' ? this.escape_column( this.generate_index_name( cols.join('_') ) ) : '' ) + ' (';

      for( let j = 0; j < cols.length; ++j )
      {
        sql += this.escape_column( cols[j] );

        if( columns[cols[j]] && ['VARCHAR', 'TEXT', 'LONGTEXT'].indexOf( columns[cols[j]].type.split(/[\s,:]+/)[0] ) > -1 )
        {
          let max_length = 255, length = 256;

          let match = columns[cols[j]].type.match(/:([0-9]+)/);
          if( match )
          {
            length = Math.min(length, parseInt(match[1]));
          }

          if( length > max_length ) { sql += '(' + max_length + ')'; }
        }

        sql += ( ( j < cols.length - 1 ) ? ',' : '' );
      }

      sql += ')';

      return sql;
    }

    this.generate_index_name = function( columns )
    {
      if( typeof columns === 'string' ) { columns = columns.split( ',' ); }
      return columns.join('_');
    }

    this.create_database = async function( database, tables, defaultRows = null, dropTables = false )
    {
      tables = JSON.parse( JSON.stringify( tables ) );

      if( database && tables )
      {
        let queries = [];

        queries.push( 'CREATE DATABASE IF NOT EXISTS ' + this.escape_column(database) + ' DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;' );
        queries.push( 'USE ' + this.escape_column(database) + ';' );

        for( let table in tables )
        {
          if( tables.hasOwnProperty( table ) )
          {
            if( dropTables ){ queries.push( 'DROP TABLE IF EXISTS ' + this.escape_column(table) + ';' ); }

            let querystring = 'CREATE TABLE IF NOT EXISTS ' + this.escape_column(table);

            let columns = [], indexes = [];

            for( let column in tables[table].columns )
            {
              if( tables[table].columns.hasOwnProperty( column ) )
              {
                columns.push( ' ' + this.escape_column( column ) + this.create_column( tables[table].columns[column] ) );
              }
            }

            for( let index in tables[table].indexes )
            {
              if( tables[table].indexes.hasOwnProperty( index ) && tables[table].indexes[index] && tables[table].indexes[index].length )
              {
                let keys = ( typeof tables[table].indexes[index] == 'string' ? [ tables[table].indexes[index] ] : tables[table].indexes[index] );

                for( let i = 0; i < keys.length; ++i )
                {
                  if(keys[i])
                  {
                    indexes.push( ' ' + this.create_index( keys[i], index, tables[table].columns ) );
                  }
                }
              }
            }

            querystring += ' (' + columns.concat( indexes ).join(',') + ' ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_general_ci;';

            queries.push( querystring );

            if( defaultRows && defaultRows.hasOwnProperty( table ) )
            {
              queries.push( defaultRows[ table ] );
            }
          }
        }

        return queries.join('\n');
      }
      else { return { ok: false, error: 'empty_data' }; }
    }

    this.modify_database = async function( modified_tables, defaultRows = null )
    {
      modified_tables = JSON.parse( JSON.stringify( modified_tables ) );

      let queries = [];
      let ctns = 0;

      return await this.database().then( async ( database ) =>
      {
        if( database.ok )
        {
          let alterTables = [];
          let tables = database.tables;

          for( let oldTableName in tables )
          {
            if( tables.hasOwnProperty( oldTableName ) && !modified_tables.hasOwnProperty( oldTableName ) )
            {
              let drop_table = true;
              for( let tableName in modified_tables )
              {
                if( modified_tables.hasOwnProperty( tableName ) && modified_tables[tableName].renamed && modified_tables[tableName].renamed === oldTableName )
                {
                  drop_table = false;
                  break;
                }
              }

              if(drop_table){ alterTables.push( 'DROP TABLE IF EXISTS ' + this.escape_column( oldTableName ) + '' ); }
            }
          }

          for( let table in modified_tables )
          {
            if( modified_tables.hasOwnProperty( table ) )
            {
              let originTableName = ( modified_tables[table]['renamed'] ? modified_tables[table]['renamed'] : table ), alterTable = '', afterUpdate = '';

              if( originTableName !== table && tables.hasOwnProperty( originTableName ) && tables.hasOwnProperty( table ) )
              {

              }

              if( !tables.hasOwnProperty( originTableName ) && tables.hasOwnProperty( table ) ){ originTableName = table; }

              if( tables.hasOwnProperty( originTableName ) )
              {
                let alterTableHeader = '  ALTER TABLE ' + this.escape_column( originTableName ) + ( originTableName !== table ? ' RENAME ' + this.escape_column( table ) : '' ) ;
                let afterUpdateHeader = '  ALTER TABLE ' + this.escape_column( table );
                let alterTableColumns = this.compare_columns( tables[ originTableName ], modified_tables[ table ], table, originTableName );
                let alterTableIndexes = this.compare_indexes( modified_tables[ table ].indexes, tables[ originTableName ].indexes, modified_tables[ table ].columns, alterTableColumns.temporaryColumns );

                if( alterTableColumns.alterColumns && alterTableColumns.alterColumns.length > 0 )
                {
                  alterTable += alterTableHeader + alterTableColumns.alterColumns.join( ',' );
                }

                if( alterTableColumns.afterUpdateColumns && alterTableColumns.afterUpdateColumns.length > 0 )
                {
                  afterUpdate += afterUpdateHeader + alterTableColumns.afterUpdateColumns.join( ',' );
                }

                if( alterTableIndexes )
                {
                  alterTable += ( alterTableColumns.alterColumns && alterTableColumns.alterColumns.length > 0 ? ', ' : alterTableHeader ) + alterTableIndexes;
                }

                let alters = [];

                if( alterTableColumns.updateBeforeAlter && alterTableColumns.updateBeforeAlter.length > 0 )
                {
                  for( let d = 0; d < alterTableColumns.updateBeforeAlter.length; d++ )
                  {
                    alters.push( alterTableColumns.updateBeforeAlter[ d ] );
                  }
                }

                if( alterTable ) { alters.push( alterTable ); }
                if( alterTableColumns.update ) { alters.push( alterTableColumns.update ); }
                if( afterUpdate ) { alters.push( afterUpdate ); }
                if( alters.length > 0 ) { ctns += alters.length; alterTables.push( alters ); }
              }
              else
              {
                ctns++;
                alterTables.push( [ this.create_table( modified_tables[table], table ) ] );
              }

              if( false && defaultRows && defaultRows.hasOwnProperty( table ) )  //TODO odobrat false
              {
                alterTables.push( defaultRows[ table ] );
              }
            }
          }

          return ( alterTables.length ? alterTables.join('; ') : 'Nothing to modify' );
        }
        else{ return { ok: false, database: database }; }
      });
    }

    this.database = async function( )
    {
      return await this.select( 'SHOW TABLES').then( async ( tables ) =>
      {
        let databaseTables = {};
        if( tables.rows && tables.rows.length > 0 )
        {
          for( let i = 0; i < tables.rows.length; i++ )
          {
            for( let info in tables.rows[ i ] )
            {
              if( tables.rows[ i ].hasOwnProperty( info ) )
              {
                let extracted = await this.extract_table( tables.rows[ i ][ info ]);

                if( extracted.ok ) { databaseTables[ extracted.name ] = extracted.table; }
              }
            }
          }

          return { ok: true, tables: databaseTables };
        }
        else { return { ok: false, error: 'empty_tables' }; }
      });
    }

    this.extract_table = async function( table )
    {
      if( table )
      {
        let tableData = {};

        return this.describe_columns( table ).then( async ( columnsData ) =>
        {
          if( columnsData.ok )
          {
            tableData.columns = columnsData.columns;
            return this.show_table_index( table ).then( async ( indexes ) =>
            {
              if( indexes.ok )
              {
                tableData.indexes = indexes.indexes;
              }

              return { ok: true, name: table, table: tableData };
            });
          }
          else { return { ok: false, error: 'empty_table' }; }
        });
      }
      else { return { ok: false, error: 'empty_table' }; }
    }

    this.compare_columns = function( current_table, modified_table, table, originTable )
    {

      let alterColumns = [], previousColumn = '', updateBeforeAlter = [], afterUpdateColumns = [], update = '', updateValues = [], temporaryColumns = {};

      for( let originColumn in current_table.columns )
      {
        if( current_table.columns.hasOwnProperty( originColumn ) )
        {
          if( !modified_table.columns.hasOwnProperty( originColumn ) )
          {
            let drop_column = true;

            for( let modiefiedColumn in modified_table.columns )
            {
              if( modified_table.columns.hasOwnProperty(modiefiedColumn) && modified_table.columns[ modiefiedColumn ].renamed && modified_table.columns[ modiefiedColumn ].renamed === originColumn )
              {
                drop_column = false;
                break;
              }
            }

            if(drop_column)
            {
              alterColumns.push( '   DROP ' + this.escape_column( originColumn ) );
            }
          }
          else
          {
            if( current_table.columns[ originColumn ].type.split(/[\s,:]+/)[0] === 'TIMESTAMP' && current_table.columns[ originColumn ].default && current_table.columns[ originColumn ].default !== 'CURRENT_TIMESTAMP' )
            {
              updateBeforeAlter.push( 'UPDATE ' + this.escape_column( originTable ) + ' SET ' + this.escape_column( originColumn ) + ' = \'2000-01-01 01:01:01\' WHERE ' + this.escape_column( originColumn ) + ' < \'1000-01-01\' ' );
            }
          }
        }
      }

      for( let column in modified_table.columns )
      {
        if( modified_table.columns.hasOwnProperty( column ) )
        {
          let originColumnName = ( modified_table.columns[ column ].renamed ? modified_table.columns[ column ].renamed : column );
          let modiefiedColumnData = this.create_column( modified_table.columns[ column ] );

          if( current_table.columns.hasOwnProperty( originColumnName ) )
          {
            let currentColumnData = this.create_column( current_table.columns[ originColumnName ] );

            if( modiefiedColumnData !== currentColumnData )
            {
              let modiefiedType = modified_table.columns[ column ].type.split(/[\s,:]+/)[0], currentType = current_table.columns[ originColumnName ].type.split(/[\s,:]+/)[0];
              let modifiedColumn = {}, setValues = '', ended = '';

              if( ( [ 'SET', 'ENUM' ].indexOf( modiefiedType ) !== -1 && modified_table.columns[column].change ) )
              {
                for( let changeValue in modified_table.columns[column].change )
                {
                  if( modified_table.columns[column].change.hasOwnProperty( changeValue ) )
                  {
                    setValues += 'IF( ' + this.escape_column( column ) + ' = ' + this.escape_value( changeValue ) + ', ' + this.escape_value( modified_table.columns[column].change[ changeValue ] ) + ', ';
                    ended += ')';
                  }
                }

                setValues += ' ' + this.escape_column( column ) + ended;
                temporaryColumns[ column ] =  'tmp_' + column;
                modifiedColumn = this.modify_column( column, modiefiedColumnData, previousColumn, setValues );
              }
              else if( currentType === 'TIMESTAMP' && modiefiedType === 'BIGINT' )
              {
                setValues += 'UNIX_TIMESTAMP( ' + this.escape_column( column ) + ' )';
                temporaryColumns[ column ] =  'tmp_' + column;
                modifiedColumn = this.modify_column( column, modiefiedColumnData, previousColumn, setValues );
              }
              else if( modified_table.columns[ column ].data && currentType !== modiefiedType )
              {
                setValues += Abstract_Connector.escape_columns( modified_table.columns[ column ], this.escape_column ) ;
                temporaryColumns[ column ] =  'tmp_' + column;
                modifiedColumn = this.modify_column( column, modiefiedColumnData, previousColumn, setValues );
              }
              else
              {
                alterColumns.push( '   CHANGE ' + this.escape_column( originColumnName ) + ' ' + this.escape_column( column ) + ' ' + this.create_column( modified_table.columns[column] ) + ( previousColumn !== '' ? ' AFTER ' + this.escape_column( previousColumn ) : '' ) );
              }

              if( modifiedColumn.alterColumns ) { alterColumns = alterColumns.concat( modifiedColumn.alterColumns ); }
              if( modifiedColumn.updateValues ) { updateValues = updateValues.concat( modifiedColumn.updateValues ); }
              if( modifiedColumn.afterUpdateColumns ) { afterUpdateColumns = afterUpdateColumns.concat( modifiedColumn.afterUpdateColumns ); }
            }
          }
          else { alterColumns.push( '   ADD ' + this.escape_column( column ) + modiefiedColumnData + ( previousColumn !== '' ? ' AFTER ' + this.escape_column( previousColumn ) : '' ) ); }

          previousColumn = column;
        }
      }

      if( updateValues.length > 0 )
      {
        update = 'UPDATE ' + this.escape_column( table ) + ' SET ' + updateValues.join( ',' );
      }

      return { updateBeforeAlter: updateBeforeAlter, alterColumns: alterColumns, update: update, afterUpdateColumns: afterUpdateColumns, temporaryColumns: temporaryColumns};
    }

    this.modify_column = function( column, modiefiedColumnData, previousColumn, setValues )
    {
      let alter = [], update = [], afterUpdate = [];
      alter.push( '   ADD ' + this.escape_column( 'tmp_' + column ) + modiefiedColumnData + ( previousColumn !== '' ? ' AFTER ' + this.escape_column( previousColumn ) : '' ) );
      update.push( this.escape_column( 'tmp_' + column ) + ' = ' + setValues );
      afterUpdate.push( '   DROP ' + this.escape_column( column ) );
      afterUpdate.push( '   CHANGE ' + this.escape_column( 'tmp_' + column ) + ' ' + this.escape_column( column ) + ' ' + modiefiedColumnData + ( previousColumn !== '' ? ' AFTER ' + this.escape_column( previousColumn ) : '' ) );
      return { alterColumns: alter, updateValues: update, afterUpdateColumns: afterUpdate };
    }

    this.compare_indexes = function( newIndex, oldIndex, columns, tmp_columns )
    {
      let alter = [], indexes = [ 'primary', 'unique', 'index' ];

      for( let i = 0; i < indexes.length; i++ )
      {
        let primary = ( indexes[i] === 'primary' ), newIndexesName = [];

        if( newIndex && newIndex[ indexes[i] ] && newIndex[ indexes[i] ].length > 0 )
        {
          for( let t = 0; t < newIndex[ indexes[i] ].length; t++ )
          {
            newIndexesName.push( ( primary ? 'PRIMARY' : this.generate_index_name( newIndex[ indexes[i] ][ t ] ) ) );
          }
        }

        if( newIndex && typeof newIndex[ indexes[i] ] === 'string' ){ newIndex[ indexes[i] ] = [ newIndex[ indexes[i] ] ]; }

        if( oldIndex && oldIndex[ indexes[i] ] )
        {
          if( Object.keys( oldIndex[ indexes[i] ] ).length > 0 )
          {
            if( newIndex && newIndex[ indexes[i] ] && newIndex[ indexes[i] ].length > 0 )
            {
              if( !primary )
              {
                if( Array.isArray( oldIndex[ indexes[i] ] ) )
                {
                  for( let u = 0; u < oldIndex[ indexes[i] ].length; u++ )
                  {
                    let exist = false;
                    for(let k = 0; k < newIndex[ indexes[i] ].length; k++ )
                    {
                      if( oldIndex[ indexes[i] ][u] && !primary && newIndex[ indexes[i] ][k] &&
                      oldIndex[ indexes[i] ][u] === newIndex[ indexes[i] ][k] )
                      {
                        exist = true;
                        break;
                      }
                    }

                    if( !exist )
                    {
                      alter.push( this.drop_index( this.generate_index_name( oldIndex[ indexes[i] ][u] ), indexes[i] ) );
                    }
                  }
                }
                else
                {
                  for( let indexName in oldIndex[ indexes[i] ] )
                  {
                    if( oldIndex[ indexes[i] ].hasOwnProperty( indexName ) )
                    {
                      let exist = false;
                      for(let k = 0; k < newIndex[ indexes[i] ].length; k++ )
                      {
                        if( oldIndex[ indexes[i] ].hasOwnProperty(indexName) && !primary && newIndex[ indexes[i] ][k] &&
                        oldIndex[ indexes[i] ][ indexName ] === newIndex[ indexes[i] ][k] )
                        {
                          exist = true;
                          break;
                        }
                      }

                      if( !exist ){ alter.push( this.drop_index( indexName, indexes[i] ) ); }
                    }
                  }
                }
              }
            }
            else{ alter.push( this.drop_index( Object.keys( oldIndex[ indexes[i] ] ), indexes[i] ) ); }
          }
        }

        if( newIndex && newIndex[ indexes[i] ] && newIndex[ indexes[i] ].length > 0 )
        {
          for( let u = 0; u < newIndex[ indexes[i] ].length; u++ )
          {
            if( newIndex[ indexes[i] ][ u ] )
            {
              let generatedIndexName = ( primary ? 'PRIMARY' : this.generate_index_name( newIndex[ indexes[i] ][ u ] ) );
              let new_index_columns = [], new_split_index_columns = newIndex[ indexes[i] ][ u ].split(',');

              for( let q = 0; q < new_split_index_columns.length; q++)
              {
                new_index_columns.push( new_split_index_columns[ q ] );
                if( tmp_columns.hasOwnProperty( new_split_index_columns[ q ] ) )
                {
                  new_index_columns.push( tmp_columns[ new_split_index_columns[ q ] ] );
                }
              }

              newIndex[ indexes[i] ][ u ] = new_index_columns.join(',');

              let changed = true;

              if( Array.isArray( oldIndex[ indexes[i] ] ) )
              {
                for( let k = 0; k < oldIndex[ indexes[i] ].length; k++ )
                {
                  if( newIndex[ indexes[i] ][ u ] === oldIndex[ indexes[i] ][ k ] ){ changed = false; }
                }
              }
              else if( typeof oldIndex[ indexes[i] ] === 'string')
              {
                if( newIndex[ indexes[i] ][ u ] === oldIndex[ indexes[i] ] ){ changed = false; }
              }
              else
              {
                for( let indexName in oldIndex[ indexes[i] ] )
                {
                  if( oldIndex[ indexes[i] ].hasOwnProperty( indexName ) )
                  {
                    if(newIndex[indexes[i]][u] === oldIndex[indexes[i]][indexName]){ changed = false; }
                  }
                }
              }

              if( changed )
              {
                if( oldIndex[ indexes[i] ].hasOwnProperty( generatedIndexName ) )
                {
                  if( oldIndex[ indexes[i] ][ generatedIndexName] !== newIndex[ indexes[i] ][ u ] )
                  {
                    alter.push( this.drop_index( generatedIndexName, indexes[i] ) );
                    alter.push( this.create_index( newIndex[ indexes[i] ][ u ], indexes[i], columns, true ) );
                  }
                }
                else{ alter.push( this.create_index( newIndex[ indexes[i] ][ u ], indexes[i], columns, true ) ); }
              }
            }
          }
        }
      }

      return ( alter.length > 0 ? ' ' + alter.join( ', ' ) : '' );
    }

    this.create_index = function( index, type, columns, alter )
    {
      if( !alter ){ alter = false; }

      let cols = index.split(/\s*,\s*/);
      let sql =  ( alter ? 'ADD ' : ' ' ) + ' ' + ( type === 'primary' ? 'PRIMARY KEY ' : ( type === 'unique' ? ( alter ? 'UNIQUE INDEX ' : 'UNIQUE KEY ' ) : ( type === 'index' ? ( alter ? 'INDEX ' : 'KEY ' ) : '' ) ) ) + ( type !== 'primary' ? this.escape_column( this.generate_index_name( cols.join('_') ) ) : '' ) + ' (';

      for( let j = 0; j < cols.length; ++j )
      {
        sql += this.escape_column( cols[j] );

        if( columns[cols[j]] && ['VARCHAR', 'TEXT', 'LONGTEXT'].indexOf( columns[cols[j]].type.split(/[\s,:]+/)[0] ) > -1 )
        {
          let max_length = 255, length = 256;

          let match = columns[cols[j]].type.match(/:([0-9]+)/);
          if( match )
          {
            length = Math.min(length, parseInt(match[1]));
          }

          if( length > max_length ) { sql += '(' + max_length + ')'; }
        }

        sql += ( ( j < cols.length - 1 ) ? ',' : '' );
      }

      sql += ')';

      return sql;
    }

    this.generate_index_name = function( columns )
    {
      if( typeof columns === 'string' ) { columns = columns.split( ',' ); }
      return columns.join('_');
    };

    this.drop_index = function( indexes, indexType )
    {
      let drop = [];
      if( indexType === 'primary' )
      {
        drop.push( 'DROP PRIMARY KEY' );
      }
      else if( typeof indexes === 'string' )
      {
        drop.push( 'DROP ' + ( indexType === 'unique' ? 'INDEX ' : 'INDEX ' ) + this.escape_column( this.generate_index_name( indexes ) ) );
      }
      else if( indexes.length )
      {
        for( let i = 0; i < indexes.length; i++ )
        {
          drop.push( 'DROP ' + ( indexType === 'unique' ? 'INDEX ' : 'INDEX ' ) + this.escape_column( indexes[ i ] ) );
        }
      }

      return drop.join( ', ' );
    }

  })();
};
