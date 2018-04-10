'use strict';

const Abstract_Connector = require( './abstract.js');
const TimedPromise = require('liqd-timed-promise');
const SQLError = require( '../errors.js');

let timeout = 15000, retrying_time = 0;
let emptied = false;
let connectionCheck = false;
let connectionLostErrors = ['PROTOCOL_CONNECTION_LOST', 'ECONNREFUSED', 'ECONNRESET'];

module.exports = function( config )
{
  return new( function()
  {
    const MYSQL_Connector = this;

    var my_sql = null,
        my_connections = null,
        my_connected = false,
	      my_container = [];

    function connect()
    {
        var options = JSON.parse(JSON.stringify(config));

        if( typeof options.charset == 'undefined' ) {   options.charset = 'utf8mb4'; }
        if( typeof options.timezone == 'undefined' ){   options.timezone = 'utc';    }
        if( typeof options.connectionLimit == 'undefined' ){ options.connectionLimit = 10; }

        options.dateStrings         = 'date';
        options.supportBigNumbers   = true;

        my_sql = require( 'mysql');
        my_connections = my_sql.createPool( options );

        my_connected = true;
        checkConnect( true );
    }
    connect();

    function checkConnect( oneTime = false )
    {
      let isConnected = ( my_connected );

      if( my_connections )
      {
        my_connections.query('SELECT \'connected\' FROM DUAL', async ( err, rows ) =>
        {
          if( err && err.code &&  connectionLostErrors.includes( err.code ) )
          {
            my_connected = false;
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
        }

        query.table = '( '+ unions.join( ' UNION ' ) +' ) ' + ( query.alias ? query.alias : 'ua_d'+ Math.ceil( Math.random() * 10000000 ) );
      }

      if( query.operation === 'select' )
      {
        if( query.order )
				{
					query.group_by = Abstract_Connector.order_with_group( query.order, query.group_by );
				}

        querystring += 'SELECT ' + Abstract_Connector.escape_columns( Abstract_Connector.expand_values( Abstract_Connector.escape_columns_group( query.columns.columns, query.group_by, query.order, 'MAX' ), query.columns.data, MYSQL_Connector.escape_value ), MYSQL_Connector.escape_column ) + ( ( query.table && query.table != 'TEMPORARY' ) ? ' FROM ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column ) : '' );
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
          querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ), MYSQL_Connector.escape_column );
        }
      }

      if( query.set )
      {
        let set = '';

        if( query.data && Array.isArray(query.data) )
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
            error         : null,
            affected_rows : 0,
            changed_rows  : 0,
            inserted_id   : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids   : [],
            row           : null,
            rows          : [],
            sql_time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
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

            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      });
    };

    this.update = function( query, callback )
    {
      if( typeof callback === 'undefined' ){ callback = null; }
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        //if( remaining_ms != 'Infinity' ){ console.log('remaining', remaining_ms ); }
        const start_time = process.hrtime();

        my_connections.query(query, ( err, rows ) =>
        {
          const elapsed_time = process.hrtime(start_time);
          let result =
          {
            ok            : true,
            error         : null,
            affected_rows : 0,
            changed_rows  : 0,
            inserted_id   : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids   : [],
            row           : null,
            rows          : [],
            sql_time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
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
              resolve( result );
            }
          }
          else
          {
            result.affected_rows = rows['affectedRows'];
            result.changed_rows = rows['changedRows'];

            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      });
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
            error         : null,
            affected_rows : 0,
            changed_rows  : 0,
            inserted_id   : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids   : [],
            row           : null,
            rows          : [],
            sql_time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
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

            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      });
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
            error         : null,
            affected_rows : 0,
            changed_rows  : 0,
            inserted_id   : null,
            inserted_ids  : [],
            changed_id    : null,
            changed_ids   : [],
            row           : null,
            rows          : [],
            sql_time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
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
              resolve( result );
            }
          }
          else
          {
            result.affected_rows = rows['affectedRows'];
            result.changed_rows = rows['changedRows'];

            if( callback ) { callback( result ); }
            else { resolve( result ); }
          }
        });
      });
    };

    this.show_table_index = function( table )
    {
      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        this.select( 'SHOW INDEX FROM ' + this.escape_column( table )).then( ( tableIndex ) =>
        {
          if( tableIndex.ok )
          {
            let indexes = { primary: {}, unique: {}, index: {} };
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

            resolve({ ok: true, indexes: indexes });
          }
          else { resolve({ ok: false, error: tableIndex.error }); }
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

              if( columnsData.rows[ k ]['Collation'] )
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
  })();
};