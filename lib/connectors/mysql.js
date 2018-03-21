'use strict';

const Abstract_Connector = require( './abstract.js');
const TimedPromise = require('liqd-timed-promise');

function sleep( ms )
{
    return new Promise( (resolve, reject) => { setTimeout( resolve, ms) } );
}

module.exports = function( config )
{
  return new( function()
  {
    const MYSQL_Connector = this;

    var s_sql = null,
        s_connections = null,
        s_connected = false,
	      check_connection = false;

    function connect()
    {
        var options = JSON.parse(JSON.stringify(config));

        if( typeof options.charset == 'undefined' ) {   options.charset = 'utf8mb4'; }
        if( typeof options.timezone == 'undefined' ){   options.timezone = 'utc';    }

        options.connectionLimit     = 10;
        options.dateStrings         = 'date';
        options.supportBigNumbers   = true;

        s_sql = require( 'mysql');
        s_connections = s_sql.createPool( options );

        s_connected = true;
    }
    connect();

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
        return ''; // TODO bud NULL, alebo prazdny string, vyskusat
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

        querystring += 'SELECT ' + Abstract_Connector.escape_columns( Abstract_Connector.expand_values( Abstract_Connector.escape_columns_group( query.columns.columns, query.group_by, query.order, '' ), query.columns.data, MYSQL_Connector.escape_value ), MYSQL_Connector.escape_column ) + ( ( query.table && query.table != 'TEMPORARY' ) ? ' FROM ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column ) : '' );
      }
      else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
      {
        querystring += 'INSERT '+( query.options && query.options.indexOf('ignore') !== -1  ? 'IGNORE' : '' )+' INTO ' + Abstract_Connector.escape_columns( query.table, MYSQL_Connector.escape_column ) + ' (' + Abstract_Connector.expand( query.columns, MYSQL_Connector.escape_column ) + ') VALUES ';

        for( let i = 0; i < query.data.length; ++i )
        {
          querystring += ( i === 0 ? '' : ',' ) + '(';

          for( let j = 0; j < query.columns.length; ++j )
          {
            querystring += ( j === 0 ? '' : ',' ) + ( typeof query.data[i][query.columns[j]] !== 'undefined' ? MYSQL_Connector.escape_value(query.data[i][query.columns[j]]) : '' );
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
        s_connections.query( query, ( err ) =>
        {
          resolve( { ok: !Boolean(err) } );
        });
      });
    };
*/
    this.select = function( query )
    {
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        //if( remaining_ms != 'Infinity' ){ console.log('remaining', remaining_ms ); }

        const start_time = process.hrtime();

        s_connections.query( query, ( err, rows ) =>
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
              time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
          };

          if( err )
          {
            result.ok = false;
            result.error = err;
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
          }

          resolve( result );
        });
      });
    };

    this.update = function( query )
    {
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
        //if( remaining_ms != 'Infinity' ){ console.log('remaining', remaining_ms ); }
        const start_time = process.hrtime();

        s_connections.query(query, ( err, rows ) =>
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
                time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
            };

            if( err )
            {
              result.ok = false;
              result.error = err;
            }
            else
            {
              result.affected_rows = rows['affectedRows'];
              result.changed_rows = rows['changedRows'];
            }

            resolve( result );
        });
      });
    };

    this.insert = function( query )
    {
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
      {
          //if( remaining_ms !== 'Infinity' ){ console.log('remaining', remaining_ms ); }
          const start_time = process.hrtime();

          s_connections.query(query, ( err, rows ) =>
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
                  time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
              };

              if( err )
              {
                result.ok = false;
                result.error = err;
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
              }

              resolve( result );
          });
      });
    };

    this.delete = function( query )
    {
      if( typeof query == 'object' ){ query = this.build( query ); }

      return new TimedPromise( ( resolve, reject, remaining_ms ) =>
      {
        const start_time = process.hrtime();

        s_connections.query( query, ( err, rows ) =>
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
              time          : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000
          };

          if( err )
          {
              result.ok = false;
              result.error = err;
          }
          else
          {
              result.affected_rows = rows['affectedRows'];
              result.changed_rows = rows['changedRows'];
          }

          resolve( result );
        });
      });
    };

    this.show_table_index = async function( table )
    {
      return await this.select( 'SHOW INDEX FROM ' + this.escape_column( table )).then( async ( tableIndex ) =>
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

            let table_indexes = {
              primary : ( indexes.primary.PRIMARY ? indexes.primary.PRIMARY  : '' ),
              unique  : Object.values( indexes.unique),
              index   : Object.values( indexes.index)
            };

            return { ok: true, indexes: table_indexes };
          }
          else { return { ok: true, indexes: indexes }; }
        }
        else { return { ok: false, error: tableIndex.error }; }
      });
    };

    this.describe_table = async function( table )
    {
      return this.select( 'SHOW FULL COLUMNS FROM ' + this.escape_column( table )).then( async ( columnsData ) =>
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

          return { ok: true, columns: columns };
        }
        else { return { ok: false, error: columnsData.error }; }
      });
    };
  })();
};
