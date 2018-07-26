'use strict';

const MAX_SAFE_DECIMALS = Math.ceil(Math.log10(Number.MAX_SAFE_INTEGER));
const MAX_UINT = '18446744073709551616';
const MIN_UINT = Number.MAX_SAFE_INTEGER.toString();

const Abstract_Connector = require( './abstract.js');
const TimedPromise = require('liqd-timed-promise');
const SQLError = require( '../errors.js');

let timeout = 15000, retrying_time = 0;
let emptied = false;
let connectionCheck = false;
let connectionLostErrors = ['ECONNREFUSED','ETIMEOUT'];

module.exports = function( config )
{
  return new( function()
  {
    const MSSQL_Connector = this;

    var convertDataType =
  	{
  		'VARCHAR'   :   'varchar',
  		'INT'       :   'numeric',
  		'TINYINT'   :   'smallint',
  		'BIGINT'    :   'numeric',
  		'DECIMAL'   :   'decimal',
  		'TIMESTAMP' :   'smalldatetime',
  		'ENUM'      :   'varchar',
  		'SET'       :   'varchar',
  		'TEXT'      :   'nvarchar'
  	};

    var usedKeyName = [];

    var ms_sql = null,
        ms_connections = null,
        ms_connected = false,
        ms_container = [];

    async function connect()
    {
      let options = JSON.parse(JSON.stringify( config ));

      ms_sql = require( '/usr/local/lib/node_modules/mssql');
    	ms_connections = await new ms_sql.ConnectionPool( options ).connect();

      checkConnect( true );
    }

    connect();

    function checkConnect( oneTime = false )
    {
      let isConnected = ( ms_connected );

      if( ms_connections )
      {
        ms_connections.request().query('SELECT \'conncetion\' "connection" ', async ( err, rows ) =>
        {
          if( err && err.code && connectionLostErrors.includes( err.code ) )
          {
            ms_connected = false;
            if( ( connectionCheck && !oneTime ) ){ connect(); }
            else if( !connectionCheck ){ setTimeout( connect, 1000 ); }
          }
          else if( !err )
          {
            ms_connected = true;
          }

          if( ms_connected && ms_container.length && !emptied ) { emptyContainer(); }

          if( !oneTime  ){ setTimeout( checkConnect, 1000 ); }
        });
      }
      else {
        ms_connected = false;
        if( ( connectionCheck && !oneTime ) ){ connect(); }
        else if( !connectionCheck ){ setTimeout( connect, 1000 ); }
      }
    }

    async function connection_state()
    {
      if( !connectionCheck ) { connectionCheck = true; checkConnect(); }

      return { connected: ms_connected };
    }

    connection_state();

    async function emptyContainer(  )
    {
      if( ms_container.length && !emptied )
      {
        emptied = true;
        let prepare_promises = [];

        do {
          let limit_conncetion = Math.min( 10, ms_container.length );

          for( let i = 0; i < limit_conncetion; i++ )
          {
            prepare_promises.push( MSSQL_Connector[ms_container[i].type]( ms_container[i].query, ms_container[i].callback ) );
          }

          ms_container.splice( 0, limit_conncetion );
        }
        while( ms_connected && ms_container.length );

        if( emptied ) { emptied = false };
      }
    }

    this.escape_column = function( column )
    {
      if(column)
      {
        return '"' + column + '"';
      }
      else { return '' };
    };

    this.escape_value = function( value )
    {
      if( typeof value === 'string' )
      {
        if( value.match(/^\d+$/) && ( value.length > MIN_UINT.length || ( value.length == MIN_UINT.length && value > MIN_UINT ) ) && ( value.length < MAX_UINT.length || ( value.length == MAX_UINT.length && value <= MAX_UINT ) ) )
        {
          return value;
        }

        value = value.replace(/\\\\/g, '\\');
        if( value.indexOf( 'NOW()' ) !== -1 ) //todo nejak doriesit
        {
          return 'getutcdate()';
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
        return null;
      }
      else if( value instanceof Buffer )
      {
        return '\'' + value.toString('hex') + '\'';  //TODO
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
            for(var i = 0; i < union.length; i++)
            {
              if( typeof union[i] === 'object' )
              {
                let dual_data = [];

                if( used_columns.length )
                {
                  for( let p = 0; p < used_columns.length; p++ )
                  {
                    dual_data.push( ( union[i].hasOwnProperty( used_columns[p] ) ? MSSQL_Connector.escape_value( union[i][ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MSSQL_Connector.escape_column)  );
                  }
                }
                else
                {
                  for( let column in union[i] )
                  {
                    if( union[i].hasOwnProperty( column ) ){ dual_data.push( MSSQL_Connector.escape_value( union[i][ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MSSQL_Connector.escape_column)  ); }
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
                dual_data.push( ( union.hasOwnProperty( used_columns[p] ) ? MSSQL_Connector.escape_value( union[ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MSSQL_Connector.escape_column)  );
              }
            }
            else
            {
              for( let column in union )
              {
                if( union.hasOwnProperty( column ) ){ dual_data.push( MSSQL_Connector.escape_value( union[ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MSSQL_Connector.escape_column)  ); }
              }
            }

            unions.push( 'SELECT ' + dual_data.join( ', ' )  );
          }
        }

        query.table = '( '+ unions.join( ' UNION ' ) +' ) ' + ( query.alias ? query.alias : 'ua_d'+ Math.ceil( Math.random() * 10000000 ) );
      }

      if( query.operation === 'select' )
      {
        let is_agregated = false;
        let escaped_columns = Abstract_Connector.escape_columns_group( query.columns.columns, query.group_by, query.order, 'MAX', is_agregated );
        is_agregated = escaped_columns.is_agregated;
        escaped_columns = escaped_columns.columns;

        if( query.order && ( query.group_by || is_agregated ) )
				{
					query.group_by = Abstract_Connector.order_with_group( query.order, query.group_by, is_agregated );
				}

        querystring += 'SELECT';

        if( !query.order && query.limit )
        {
          querystring += ' TOP ' + query.limit;
        }

        querystring += ' ' + Abstract_Connector.escape_columns( Abstract_Connector.expand_values( escaped_columns, query.columns.data, MSSQL_Connector.escape_value ), MSSQL_Connector.escape_column ) + ( ( query.table && query.table != 'TEMPORARY' ) ? ' FROM ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column ) : '' );
      }
      else if( query.operation === 'insert' && query.options && query.options.indexOf('ignore') !== -1 )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
      {
        let queryColumn = query.columns;
        querystring += 'INSERT INTO ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column ) + ' (' + Abstract_Connector.expand( queryColumn, MSSQL_Connector.escape_column ) + ')';
        querystring += 'SELECT * FROM (';

        let not_exist_select = ' SELECT '+ Abstract_Connector.expand( queryColumn, MSSQL_Connector.escape_column ) + ' FROM ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column ) + ' WHERE ';
        for( let i = 0; i < query.data.length; ++i )
        {
          querystring += ( i === 0 ? '' : ' UNION ' ) + ' ( ';

          for( let j = 0; j < queryColumn.length; ++j )
          {
            if( i === 0 )
            {
              not_exist_select += ( j === 0 ? '' : ' AND ' ) + ' ' + Abstract_Connector.escape_columns( 'ins_union.' + queryColumn[j], MSSQL_Connector.escape_column ) + ' = ' + Abstract_Connector.escape_columns( query.table + '.' + queryColumn[j], MSSQL_Connector.escape_column );
            }

            querystring += ( j === 0 ? ' SELECT ' : ',' ) + MSSQL_Connector.escape_value( ( ( query.data[i][queryColumn[j]] || query.data[i][queryColumn[j]] === 0 )  ? query.data[i][queryColumn[j]] : null ) ) + ' ' + Abstract_Connector.escape_columns( queryColumn[j], MSSQL_Connector.escape_column );
          }
          querystring += ' ) ';
        }

        querystring += '	) ' + Abstract_Connector.escape_columns( 'ins_union' , MSSQL_Connector.escape_column );
        querystring += ' WHERE ';
        querystring += '	NOT EXISTS ( ' + not_exist_select + ' )';
      }
      else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
      {
        querystring += 'INSERT INTO ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column ) + ' (' + Abstract_Connector.expand( query.columns, MSSQL_Connector.escape_column ) + ') VALUES ';

        for( let i = 0; i < query.data.length; ++i )
        {
          querystring += ( i == 0 ? '' : ',' ) + '(';

          for( let j = 0; j < query.columns.length; ++j )
          {
            querystring += ( j == 0 ? '' : ',' ) + ( typeof query.data[i][query.columns[j]] !== 'undefined' ? MSSQL_Connector.escape_value(query.data[i][query.columns[j]]) : null );
          }

          querystring += ')';
        }
      }
      else if( query.operation === 'update' )
      {
        querystring += 'UPDATE ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column );
      }
      else if( query.operation === 'delete' )
      {
        querystring += 'DELETE FROM ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column );
      }

      if( query.join )
      {
        for( var i = 0; i < query.join.length; ++i )
        {
          //querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ), MSSQL_Connector.escape_column );

          if( typeof query.join[i].table === 'object' && query.join[i].table )
          {
            if( !query.join[i].table.columns ){ query.join[i].table.columns = { columns: '*' , data: null }; }

            query.join[i].table.operation = 'select';
            let subquery = this.build( query.join[i].table );
            querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN (' + subquery + ') ' + query.join[i].table.alias + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ), MSSQL_Connector.escape_column );
          }
          else
          {
            querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ), MSSQL_Connector.escape_column );
          }
        }
      }

      if( query.set )
      {
        var set = '';


        if( query.operation === 'update' && query.hasOwnProperty( 'update_with_where' ) && query.update_with_where )  //TODO test this
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

            set += ( i ? ', ' : '' ) + MSSQL_Connector.escape_column(columns[i]) + ' = CASE';

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
                    for( var index of groups_indexes )
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
                  set += ( k ? ' AND ' : '' ) + MSSQL_Connector.escape_column(indexes[k]) + ' = ' + MSSQL_Connector.escape_value(query.data[j][indexes[k]]);
                }

                set += ' THEN ' + MSSQL_Connector.escape_value(query.data[j][columns[i]]);
              }
            }

            set += ' ELSE ' + MSSQL_Connector.escape_column(columns[i]) + ' END';
          }
        }
        else
        {
          set = Abstract_Connector.escape_columns(query.set, MSSQL_Connector.escape_column);

          if(query.data !== null)
          {
            set = Abstract_Connector.expand_values(set, query.data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column);
          }
        }

        querystring += ' SET ' + set;
      }

      if( query.where )
      {
        let where = '';

        for( let i = 0; i < query.where.length; ++i )
        {
          let condition = Abstract_Connector.escape_columns( query.where[i].condition, MSSQL_Connector.escape_column );

          if( typeof query.where[i].data !== 'undefined' )
          {
            condition = Abstract_Connector.expand_values( condition, query.where[i].data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column );
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
        let condition = Abstract_Connector.escape_columns( query.group_by.condition, MSSQL_Connector.escape_column );
        querystring += ' GROUP BY ' + ( query.group_by.data ? Abstract_Connector.expand_values( condition, query.group_by.data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ) : condition );
      }

      if( query.having )
      {
        let condition = Abstract_Connector.escape_columns( query.having.condition, MSSQL_Connector.escape_column );
        querystring += ' HAVING ' + ( query.having.data ? Abstract_Connector.expand_values( condition, query.having.data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ) : condition );
      }

      if( query.order )
      {
        let condition = Abstract_Connector.escape_columns( query.order.condition, MSSQL_Connector.escape_column );
        querystring += ' ORDER BY ' + ( query.order.data ? Abstract_Connector.expand_values( condition, query.order.data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ) : condition );
      }

      if( query.offset && query.order )
      {
        querystring += ' OFFSET ' + query.offset + ' ROWS';

        if( query.limit )
        {
          querystring += ' FETCH NEXT '+query.limit+' ROWS ONLY';
        }
      }
      else if( query.order && query.limit )
      {
        querystring += ' OFFSET 0 ROWS';
        querystring += ' FETCH NEXT '+query.limit+' ROWS ONLY';
      }

      return querystring;
    };
/*
    this.query = function( query )
    {
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        ms_connections.request().query( query, ( err ) =>
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

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        if( ms_connections )
        {
          const start_time = process.hrtime();

          ms_connections.request().query( query , (err, rows) =>
          {
            const elapsed_time = process.hrtime(start_time);

            let result =
            {
              ok: true,
              error: null,
              affected_rows: 0,
              changed_rows: 0,
              inserted_id: null,
              inserted_ids: [],
              changed_id: null,
              changed_ids: [],
              row: null,
              rows: [],
              sql_time: elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
            };

            if( err )
            {
              if( connectionLostErrors.includes( err.code ) )
	            {
		            ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'select' });
	            }
              else
              {
                result.ok = false;
                result.error = new SQLError( err ).get();
                resolve( result );
              }
            }
            else
            {
              if( rows.recordset )
              {
                for( var i = 0; i < rows.recordset.length; ++i )
                {
                  for( let column in rows.recordset[i] )
                  {
                    if( Array.isArray( rows.recordset[i][ column ] ) ){ rows.recordset[i][ column ] = rows.recordset[i][ column ][ rows.recordset[i][ column ].length - 1 ] }
                  }

                  result.rows.push( rows.recordset[i] );
                }
              }

              if( result.rows && result.rows.length )
              {
                result.row = result.rows[0];
              }

              result.affected_rows = rows.rowsAffected || result.rows.length;

              if( callback ) { callback( result ); }
              else { resolve( result ); }
            }
          });
        }
        else { ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'select' }); };
      });
    };

    this.update = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        if( ms_connections )
        {
          const start_time = process.hrtime();

          ms_connections.request().query( query , (err, rows) =>
          {
            const elapsed_time = process.hrtime(start_time);

            let result =
            {
              ok: true,
              error: null,
              affected_rows: 0,
              changed_rows: 0,
              inserted_id: null,
              inserted_ids: [],
              changed_id: null,
              changed_ids: [],
              row: null,
              rows: [],
              sql_time: elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
            };

            if( err )
            {
              if( connectionLostErrors.includes( err.code ) )
	            {
		            ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'update' });
	            }
              else
              {
                result.ok = false;
                result.error = new SQLError( err ).get();;
                resolve( result );
              }
            }
            else
            {
              if( rows.hasOwnProperty( 'rowsAffected' ) && Array.isArray( rows.rowsAffected ) && rows.rowsAffected[0] )
              {
                result.affected_rows = rows.rowsAffected[0];
                result.changed_rows = rows.rowsAffected[0];
              }

              if( callback ) { callback( result ); }
              else { resolve( result ); }
            }
          });
        }
        else { ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'update' }); }
      });
    };

    this.insert = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      query += '; ' + Abstract_Connector.escape_columns( 'SELECT SCOPE_IDENTITY() AS id', MSSQL_Connector.escape_column );

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        if( ms_connections )
        {
          const start_time = process.hrtime();

          ms_connections.request().query( query , (err, rows) =>
          {
            const elapsed_time = process.hrtime(start_time);

            let result =
            {
              ok: true,
              error: null,
              affected_rows: 0,
              changed_rows: 0,
              inserted_id: null,
              inserted_ids: [],
              changed_id: null,
              changed_ids: [],
              row: null,
              rows: [],
              sql_time: elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
            };

            if( err )
            {
              if( connectionLostErrors.includes( err.code ) )
	            {
		            ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'insert' });
	            }
              else
              {
                result.ok = false;
                result.error = new SQLError( err ).get();;
                resolve( result );
              }
            }
            else
            {
              result.changed_rows = result.affected_rows = rows['affectedRows'];  //TODO prerobit

              if( rows.rowsAffected && Array.isArray( rows.rowsAffected ) && rows.rowsAffected[0] )
              {
                result.changed_rows = result.affected_rows = rows.rowsAffected[0];

                let new_insertedID = ( rows.recordset.length ? rows.recordset[0].id : null );

                if( new_insertedID && rows.rowsAffected[0] )
                {
                  for( let i = 0; i < result.affected_rows; ++i )
                  {
                    result.inserted_ids.push( ( new_insertedID - result.affected_rows + 1 ) + i );
                    result.changed_ids.push( ( new_insertedID - result.affected_rows + 1 ) + i );
                  }

                  result.inserted_id = result.changed_id = result.inserted_ids[0];
                }
              }

              if( callback ) { callback( result ); }
              else { resolve( result ); }
            }
          });
        }
        else { ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'insert' }); }
      });
    };

    this.delete = async function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        if( ms_connections )
        {
          const start_time = process.hrtime();

          ms_connections.request().query( query , (err, rows) =>
          {
            const elapsed_time = process.hrtime(start_time);

            let result =
            {
              ok: true,
              error: null,
              affected_rows: 0,
              changed_rows: 0,
              inserted_id: null,
              inserted_ids: [],
              changed_id: null,
              changed_ids: [],
              row: null,
              rows: [],
              sql_time: elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
            };

            if( err )
            {
              if( connectionLostErrors.includes( err.code ) )
	            {
		            ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'delete' });
	            }
              else
              {
                result.ok = false;
                result.error = new SQLError( err ).get();
                resolve( result );
              }
            }
            else
            {
              if( rows.rowsAffected && Array.isArray( rows.rowsAffected ) && rows.rowsAffected[0] )
              {
                result.changed_rows = result.affected_rows = rows.rowsAffected[0];
              }

              if( callback ) { callback( result ); }
              else { resolve( result ); }
            }
          });
        }
        else { ms_container.push( { query: query, callback: ( rowed ) => { resolve( rowed ); }, type: 'delete' }); }
      });
    };

    this.show_table_index = async function( table )
    {
      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        this.select('SELECT OBJECT_NAME(ind.object_id) AS ObjectName, ind.name AS IndexName, ind.is_primary_key AS IsPrimaryKey, ind.is_unique AS IsUniqueIndex, col.name AS ColumnName, ic.is_included_column AS IsIncludedColumn, ic.key_ordinal AS ColumnOrder\n' +
                    'FROM       sys.indexes ind\n' +
                    'INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id\n' +
                    'INNER JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id\n' +
                    'INNER JOIN sys.tables t ON ind.object_id = t.object_id\n' +
                    'WHERE  t.is_ms_shipped = 0 AND ind.object_id = object_id(' + this.escape_value( table ) + ')\n' +
                    'ORDER BY OBJECT_NAME(ind.object_id), ind.is_unique DESC, ind.name, ic.key_ordinal' ).then( (tableIndex) =>
        {
          let indexes = { primary: {}, unique: {}, index: {} };

          if( tableIndex.ok )
          {
            let indexData = { primary: {}, unique: {}, index: {} };

            if( tableIndex.rows && tableIndex.rows.length )
            {
              for( let j = 0; j < tableIndex.rows.length; j++ )
              {
                let indexType = ( tableIndex.rows[j]['IsPrimaryKey'] ? 'primary' : ( tableIndex.rows[j]['IsUniqueIndex'] ? 'unique' : 'index' ) );

                if( indexType === 'primary' ){ tableIndex.rows[j]['IndexName'] = 'PRIMARY'; }

                if( !indexData[ indexType ].hasOwnProperty( tableIndex.rows[j]['IndexName'] ) )
                {
                  indexData[ indexType ][ tableIndex.rows[j]['IndexName'] ] = [];
                }

                indexData[ indexType ][ tableIndex.rows[j]['IndexName'] ].push( tableIndex.rows[j]['ColumnName'] );
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
              };

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

    this.describe_columns = async function( table )
    {
      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        let columns = {}, identity = {}, constraint = {};
        var convertDataType =
        {
          'nvarchar'      : 'VARCHAR',
          'numeric'	      : 'INT',
          'decimal'       : 'DECIMAL',
          'smalldatetime' : 'TIMESTAMP',
          'smallint'      : 'TINYINT',
          'int'           : 'INT',
          'bigint'        : 'BIGINT',
          'ntext'         : 'TEXT',
          'text'          : 'TEXT',
        };

        this.select( 'SELECT * FROM .INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N' + this.escape_value( table )).then( (columnsData) =>
        {
          if( columnsData.rows && columnsData.rows.length )
          {
            this.select( 'SELECT name, is_identity FROM sys.columns WHERE object_id = object_id(' + this.escape_value( table ) + ') ').then( (isIdentity) =>
            {
              if( isIdentity.ok )
              {
                if( isIdentity.rows.length ){ isIdentity.rows.forEach( ( column_name ) => { identity[ column_name.name ] = ( column_name.is_identity ); }); }

                this.select( 'SELECT TableName = t.Name, ColumnName = c.Name, dc.Name, dc.definition FROM sys.tables t INNER JOIN sys.check_constraints dc ON t.object_id = dc.parent_object_id INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND c.column_id = dc.parent_column_id WHERE t.object_id = object_id(' + this.escape_value( table ) + ') ORDER BY t.Name').then( (haveConstraint) =>
                {
                  if( haveConstraint.ok )
                  {
                    if( haveConstraint.rows.length ){ haveConstraint.rows.forEach( ( column_name ) => { constraint[ column_name.ColumnName ] = ( column_name.definition ); }); }

                    for( let k = 0; k < columnsData.rows.length; k++ )
                    {
                      let columnName = columnsData.rows[ k ]['COLUMN_NAME'], precision = true;

                      columns[ columnName ] = {};

                      if( columnsData.rows[ k ]['NUMERIC_PRECISION'] === 20 )
                      {
                        columns[ columnName ].type = 'BIGINT';
                      }
                      else if( columnsData.rows[ k ]['NUMERIC_PRECISION'] === 11 )
                      {
                        columns[ columnName ].type = 'INT';
                      }
                      else if( columnsData.rows[ k ]['NUMERIC_PRECISION'] === 3 )
                      {
                        columns[ columnName ].type = 'TINYINT';
                      }
                      else
                      {
                        if( columnsData.rows[ k ]['CHARACTER_MAXIMUM_LENGTH'] > 100000 )
                        {
                          columns[ columnName ].type = 'TEXT';
                          precision = false;
                        }
                        else
                        {
                          columns[ columnName ].type = convertDataType[columnsData.rows[ k ]['DATA_TYPE']];
                        }
                      }

                      if( constraint.hasOwnProperty( columnName ) )
                      {
                        if( [ 'numeric', 'smallint' ].indexOf( columnsData.rows[ k ]['DATA_TYPE'] ) !== -1 )
                        {
                          precision = false;
                          columns[ columnName ].type +=  ':UNSIGNED';
                        }
                        else if( ['nvarchar','varchar'].indexOf( columnsData.rows[ k ]['DATA_TYPE'] ) !== -1 )
                        {
                          columns[ columnName ].type = 'VARCHAR';
                          let match = constraint[ columnName ].match(/'(.*?)'/g );

                          if( match && match.length )
                          {
                            precision = false;

                            if( ( match.join( ',' ).replace(/'/g, '').length + 2 ) === columnsData.rows[ k ]['CHARACTER_MAXIMUM_LENGTH'] )
                            {
                              columns[ columnName ].type = 'SET'
                            }
                            else
                            {
                              columns[ columnName ].type = 'ENUM'
                            }

                            columns[ columnName ].type +=  ':' + match.join( ',' ).replace(/'/g, '');
                          }
                        }
                      }

                      if( columnsData.rows[ k ]['CHARACTER_MAXIMUM_LENGTH'] && precision )
                      {
                        columns[ columnName ].type += ':' + columnsData.rows[ k ]['CHARACTER_MAXIMUM_LENGTH'];
                      }
                      else if( columnsData.rows[ k ]['NUMERIC_PRECISION'] && precision )
                      {
                        columns[ columnName ].type += ':' + columnsData.rows[ k ]['NUMERIC_PRECISION'] + ( columnsData.rows[ k ]['NUMERIC_SCALE'] ? ','+columnsData.rows[ k ]['NUMERIC_SCALE'] : '' );
                      }

                      if( columnsData.rows[ k ]['IS_NULLABLE'] === 'YES' )
                      {
                        columns[ columnName ].null = true;
                      }

                      if( columnsData.rows[ k ]['COLUMN_DEFAULT'] )
                      {
                        let match = columnsData.rows[ k ]['COLUMN_DEFAULT'] .match(/\((.*?)\)/)
                        if( match )
                        {
                          columns[ columnName ].default = match[1].replace(/'/g, '');
                        }
                      }

                      if( columnsData.rows[ k ]['Extra'] === 'on update CURRENT_TIMESTAMP' ) // nepouziva sa
                      {
                        columns[ columnName ].update = 'CURRENT_TIMESTAMP';
                      }
                      else if( identity.hasOwnProperty( columnName ) && identity[ columnName ] )
                      {
                        columns[ columnName ].increment = true;
                      }
                    }

                    resolve({ ok: true, columns: columns });
                  }
                  else { resolve({ ok: false, error: haveConstraint.error }); }
                })
              }
              else { resolve({ ok: false, error: isIdentity.error }); }
            });
          }
          else if( columnsData.ok ) { resolve({ ok: false, error: new SQLError( 'UNDEFINED_TABLE' ).get() }); }
          else { resolve({ ok: false, error: columnsData.error }); }
        });
      });
    };

    this.create_table = function( table, name )
  	{
  		let columns = [], indexes = [];
  		let querystring = 'IF ( NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ' + this.escape_value('dbo') + ' AND  TABLE_NAME = ' + this.escape_value( name ) + ')) BEGIN CREATE TABLE ' + this.escape_column( name );

  		for( let column in table.columns )
  		{
  			if( table.columns.hasOwnProperty( column ) )
  			{
  				let columnData =  ' ' + this.escape_column( column ) + ' ' + this.create_column( table.columns[ column ], column, name );

          columns.push( columnData );
  			}
  		}

  		for( let type in table.indexes )
  		{
  			if( table.indexes.hasOwnProperty(type) && table.indexes[type] && table.indexes[type].length > 0 )
  			{
  				let keys = ( typeof table.indexes[type] === 'string' ? [ table.indexes[type] ] : table.indexes[type] );

  				for( let i = 0; i < keys.length; i++ )
  				{
  					let alterTableIndexes = this.create_index( keys[i], type, table.columns, name );

  					if( alterTableIndexes )
  					{
  						indexes.push( alterTableIndexes );
  					}
  				}
  			}
  		}

  		querystring += ' (' + columns.concat( indexes ).join(',') + ' ) ; END';

  		return querystring;
  	}

    this.drop_table = function( table )
  	{
  		let querystring = 'IF ( EXISTS ( SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ' + this.escape_value('dbo') + ' AND  TABLE_NAME = ' + this.escape_value( table ) + ')) BEGIN DROP TABLE ' + this.escape_column( table ) + '; END';
  		return querystring;
  	}

    this.create_column = function( columnData, columnName, table  )
    {
      let column = '';

      if( columnData )
      {
        if( columnData['type'] )
        {
          let type = columnData['type'].split(/[\s,:]+/)[0];

          if( ( [ 'VARCHAR', 'ENUM', 'SET' ].indexOf(type) !== -1 && columnData['type'].toLowerCase().indexOf('multibyte') !== -1 ) || columnData['multibyte'] )
          {
            column += ' n' + convertDataType[type];
          }
          else
          {
            column += ' ' + convertDataType[type];
          }

          let size = columnData['type'].match(/:([0-9]+)/);

          if( type === 'DECIMAL' )
          {
            column += '('+columnData['type'].match(/:([0-9,]+)/)[1]+')';
          }
          else if( type === 'TEXT' )
          {
            column += '(max)';
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
          //column += '(3)';
          }
          else if( type === 'ENUM' )
          {
            let max_char_length = 0;
            let column_allowed_strings = columnData['type'].split(':')[1].trim().split(/\s*,\s*/);

            if( column_allowed_strings.length )
            {
              for( let i = 0; i < column_allowed_strings.length; i++ )
              {
                if( column_allowed_strings[i].length > max_char_length ) { max_char_length = column_allowed_strings[i].length; }
              }
            }

            column += '(' + ( max_char_length < 10 ? ( ( max_char_length === 9 ? max_char_length + 1 : max_char_length + 2 ) ) : max_char_length ) + ')';
          }
          else if( type === 'SET' )
          {
            let column_allowed_strings = columnData['type'].split(':')[1].trim();
            column += '(' + ( column_allowed_strings.length + 2 ) + ')';
          }
          else if( size )
          {
            column += '(' + size[1] + ')';
          }

          if( ( [ 'VARCHAR' ].indexOf(type) !== -1 && columnData['type'].toLowerCase().indexOf('multibyte') !== -1 ) || columnData['multibyte'] )
          {

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
          //	column += ' ON UPDATE ' + ( ['CURRENT_TIMESTAMP'].indexOf(columnData['update']) === -1 ? this.escape_value( columnData['update'] ) : columnData['update'] );
          }

          if( columnData['increment'] )
          {
            column += ' IDENTITY(1,1)';
          }

          if( ['SET', 'ENUM'].indexOf( type ) > -1 )
          {
            column += ' CONSTRAINT ' + this.generateConstraintName( columnName, table, null, 'cons' ) + ' CHECK ( ' + this.escape_column(columnName) + ' IN (' + Abstract_Connector.expand( columnData['type'].split(':')[1].trim().split(/\s*,\s*/), this.escape_value ) + ') )';
          }

          if( ( [ 'INT', 'BIGINT', 'TINYINT' ].indexOf(type) !== -1 && columnData['type'].toUpperCase().indexOf(':UNSIGNED') !== -1 ) || columnData['unsigned']  )
          {
            column += ' CONSTRAINT ' + this.generateConstraintName( columnName, table, null, 'uns' ) + ' check ( ' + this.escape_column(columnName) + ' >= 0 )';
          }
        }
      }

      return column;
    };

    this.create_index = function( index, type, columns, table, alter ) //todo
    {
      if( !alter ){ alter = false; }
      let sql = '';
      let cols = index.split(/\s*,\s*/);

      sql =  ( alter ? 'ADD ' : ' ' ) + ( ['primary', 'unique'].indexOf( type ) === -1 ? 'INDEX ' : ' CONSTRAINT ' ) + this.generateConstraintName( cols.join(','), table, ( type === 'primary' ? 'PK' : ( type === 'unique' ? 'UC' : 'C' )  ), null ) + ' ' + ( type === 'primary' ? 'PRIMARY KEY ' : ( type === 'unique' ? ( alter ? 'UNIQUE ' : 'UNIQUE ' ) : ( type === 'index' ? ( alter ? 'INDEX ' : ' ' ) : '' ) ) ) + ' (';

      for( let j = 0; j < cols.length; j++ )
      {
        sql += this.escape_column( cols[j] );

        if( columns[cols[j]] && ['VARCHAR', 'TEXT'].indexOf( columns[cols[j]].type.split(/[\s,:]+/)[0] ) > -1 )
        {
          let max_length = 255, length = 256;

          let match = columns[cols[j]].type.match(/:([0-9]+)/);
          if( match )
          {
          //	length = Math.min(length, parseInt(match[1]));
          }
        }

        sql += ( ( j < cols.length - 1 ) ? ',' : '' );
      }

      sql += ')';

      return sql;
    };

    this.generateConstraintName = function( columns, table, prefix, type )
    {
      let constraintName = [];

      if( prefix ) { constraintName.push( prefix ); }
      if( table )
      {
        constraintName.push( table );
      }

      if( typeof columns === 'string' )
      {
        columns = columns.split( ',' );
      }

      if( Array.isArray( columns ) )
      {
        for( let i = 0; i < columns.length; i++ )
        {
          if( columns[ i ].length > 4 )
          {
            let splitedColumn = columns[ i ].split( '_' ), joinedColumn = '';

            for( let n = 0; n < splitedColumn.length; n++ )
            {
              if( splitedColumn[n] )
              {
                joinedColumn += splitedColumn[n].substr( 0, 4 ).toUpperCase();
              }
            }

            constraintName.push( joinedColumn );
          }
          else { constraintName.push( columns[ i ] ); }
        }

        if( type ) { constraintName.push( type ); }

        usedKeyName.push( constraintName.join('_') );
        return this.escape_column( constraintName.join('_') );
      }
      else { return ''; }
    };

  })();
};
