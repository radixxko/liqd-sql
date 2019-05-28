'use strict';

var Abstract_Connector = require( './abstract.js')({
	escape_keyword: '\\'
});
const TimedPromise = require('liqd-timed-promise');
const SQLError = require( '../errors.js');

const MAX_SAFE_DECIMALS = Math.ceil(Math.log10(Number.MAX_SAFE_INTEGER));
const MAX_UINT = '18446744073709551616';
const MIN_UINT = Number.MAX_SAFE_INTEGER.toString();

const BIND_IF_FLOW = callback => typeof LIQD_FLOW !== 'undefined' && LIQD_FLOW.started ? LIQD_FLOW.bind( callback ) : callback;

function getMiliseconds()
{
	return (new Date()).getTime();
}

module.exports = function( config, emit )
{
	return new( function()
	{
		const MSSQL_Connector = this;

		let convertDataType =
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
		ms_connected = false;

		this.ping_query = function()
		{
			if( ms_connections )
			{
				ms_connections.request().query('SELECT \'conncetion\' "connection" ', (err, result) =>
				{
					if( !err && !ms_connected )
					{
						ms_connected = true;
						emit( 'status', 'connected' );
					}
					else if( err &&  ms_connected )
					{
						ms_connected = false;
						emit( 'status', 'disconnected' );
					}
				});
			}
			else
			{
				ms_connected = false;
				emit( 'status', 'disconnected' );
			}
		}

		async function connect()
		{
			let options = JSON.parse(JSON.stringify( config ));

			try
			{
				ms_sql = require( 'mssql');
			}
			catch(e){ throw new Error('Please install "mssql" module to use mssql connector'); }

			if( config.non_escape_keywords && Array.isArray( config.non_escape_keywords ) && config.non_escape_keywords.length ){ Abstract_Connector.abstract_keywords = Abstract_Connector.abstract_keywords.concat( config.non_escape_keywords ); }

			ms_connections = await new ms_sql.ConnectionPool( options ).connect().catch( err =>
			{
				if( err )
				{
					throw new Error( err );
				}

				setTimeout( connect, 1000 );
			});

			if( ms_connections )
			{
				ms_connections.on( 'error', err =>
				{
					if( err && connectionLostErrors.includes( err.code ) && ms_connected )
					{
						ms_connected = false;
						emit( 'status', 'disconnected' );
					}
				});
			}
		}

		function aggregate_columns( columns, aggregator = 'MAX' )
		{
			let columnRE = /^(("[^"]+"\.)*("[^"]+"))$/m;
			let columnsRE = /^(\s*(("[^"]+"\.)*("[^"]+"))\s*,)+\s*(("[^"]+"\.)*("[^"]+"))\s*$/m;
			let aggregationRE = /((AVG|BIT_AND|BIT_OR|BIT_XOR|CHECKSUM_AGG|COUNT|COUNT_BIG|GROUP_CONCAT|GROUPING|GROUPING_ID|JSON_ARRAYAGG|JSON_OBJECTAGG|MAX|MIN|STD|STDDEV|STDDEV_POP|STDDEV_SAMP|STRING_AGG|SUM|VAR|VARP|VAR_POP|VAR_SAMP|VARIANCE)\s*\((\s*DISTINCT(\s*\(|\s+){0,1}){0,1}|([\)0-9'""]{0,1}))\s*?(("[^"]+"\.)*"[^"]+")/gm;
			let stringsRE = /('[^']*'|"[^"]*")/gm;
			let column, lastOffset = null, lastAggregatedDistinct = null;

			return columns.replace( aggregationRE, ( match, _1, aggregated, distinct, _3, alias, column, _4, offset ) =>
			{
				let aggregate = !( aggregated || alias || lastOffset === offset ); lastOffset = offset + match.length;

				if( aggregate )
				{
					if( !( lastAggregatedDistinct && columnsRE.test( columns.substring( lastAggregatedDistinct, lastOffset ))) )
					{
						lastAggregatedDistinct = null;

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
				}
				else if( distinct )
				{
					lastAggregatedDistinct = lastOffset - column.length;
				}

				return match;
			});
		}

		this.escape_column = function( column )
		{
			if(column)
			{
				return '"' + column + '"';
			}
			else { return ''; }
		};

		this.transform_function = function( transform, position )
		{
			if( transform.hasOwnProperty( position ) && typeof transform[position] === 'object' && transform[position].name )
			{
				if( transform[position].name.toUpperCase() === 'IF' )
				{
					let label = 'IIF( '+ transform[position].values.join(', ') +')'; //todo kontrola na pocet

					return label;
				}
				else if( transform[position].name.toUpperCase() === 'NOW' )
				{
					return 'GETUTCDATE()';
				}
				else if( transform[position].name.toUpperCase() === 'UNIX_TIMESTAMP' )
				{
					return 'DATEDIFF(s, \'1970-01-01\', GETUTCDATE()';
				}
				else { return null; }
			}
			else { return null; }
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
					if( value.indexOf( 'INTERVAL' ) !== -1 )
					{
						let [ , operator, , duration, interval ] = value.match(/([+-]+)\s*(INTERVAL\s+(\d*?)\s+([a-z]+))/i);
						let new_interval = ( interval.toUpperCase === 'HOUR' ? 'hh' : 'hh' );

						return 'DATEADD('+ interval +','+ ( operator === '-' ? '-' : '' ) + duration +',GETUTCDATE())';
					}
					else { return 'GETUTCDATE()'; }
				}
				else if( value.indexOf( 'UNIX_TIMESTAMP()') !== -1 )
				{
					return 'DATEDIFF(s, \'1970-01-01\', GETUTCDATE())';
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
			else if( typeof value === 'object' )
			{
				return '\'' + JSON.stringify( value ) + '\'';
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

		this.build = function( query, auto_increment = false )
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
						for(let i = 0; i < union.length; i++)
						{
							if( typeof union[i] === 'object' )
							{
								let dual_data = [];

								if( used_columns.length )
								{
									for( let p = 0; p < used_columns.length; p++ )
									{
										dual_data.push( ( union[i].hasOwnProperty( used_columns[p] ) ? MSSQL_Connector.escape_value( union[i][ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MSSQL_Connector.escape_column, MSSQL_Connector.transform_function)  );
									}
								}
								else
								{
									for( let column in union[i] )
									{
										if( union[i].hasOwnProperty( column ) ){ dual_data.push( MSSQL_Connector.escape_value( union[i][ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function)  ); }
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
								dual_data.push( ( union.hasOwnProperty( used_columns[p] ) ? MSSQL_Connector.escape_value( union[ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MSSQL_Connector.escape_column, MSSQL_Connector.transform_function)  );
							}
						}
						else
						{
							for( let column in union )
							{
								if( union.hasOwnProperty( column ) ){ dual_data.push( MSSQL_Connector.escape_value( union[ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function)  ); }
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

			if( query.operation === 'insert' && auto_increment )
			{
				querystring += '; SET IDENTITY_INSERT ' + Abstract_Connector.escape_columns( query.table , MSSQL_Connector.escape_column ) + ' ON;';
			}

			if( query.operation === 'select' )
			{
				let escaped_columns = Abstract_Connector.escape_columns( Abstract_Connector.expand_values( query.columns.columns, query.columns.data, MSSQL_Connector.escape_value ), MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );

				if( query.group_by || query.having )
				{
					escaped_columns = aggregate_columns( escaped_columns, 'MAX' );
				}

				querystring += 'SELECT '
				if( !query.order && query.limit )
				{
					querystring += ' TOP ' + query.limit + ' ';
				}


				querystring += escaped_columns + ( ( query.table && !['DUAL','TEMPORARY'].includes(query.table) ) ? ' FROM ' + Abstract_Connector.escape_columns( query.table + ( query.table_alias ? ' ' + query.table_alias : '' ), MSSQL_Connector.escape_column, MSSQL_Connector.transform_function ) : '' );
			}
			else if( query.operation === 'insert' && query.options && query.options.indexOf('ignore') !== -1 )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
			{


				let queryColumn = query.columns;
				querystring += 'INSERT INTO ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function ) + ' (' + Abstract_Connector.expand( queryColumn, MSSQL_Connector.escape_column ) + ')';
				querystring += 'SELECT * FROM (';

				let not_exist_select = ' SELECT '+ Abstract_Connector.expand( queryColumn, MSSQL_Connector.escape_column ) + ' FROM ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function ) + ' WHERE ';
				for( let i = 0; i < query.data.length; ++i )
				{
					querystring += ( i === 0 ? '' : ' UNION ' ) + ' ( ';

					for( let j = 0; j < queryColumn.length; ++j )
					{
						if( i === 0 )
						{
							not_exist_select += ( j === 0 ? '' : ' AND ' ) + ' ' + Abstract_Connector.escape_columns( 'ins_union.' + queryColumn[j], MSSQL_Connector.escape_column, MSSQL_Connector.transform_function ) + ' = ' + Abstract_Connector.escape_columns( query.table + '.' + queryColumn[j], MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
						}

						querystring += ( j === 0 ? ' SELECT ' : ',' ) + MSSQL_Connector.escape_value( ( ( query.data[i][queryColumn[j]] || query.data[i][queryColumn[j]] === 0 )  ? query.data[i][queryColumn[j]] : null ) ) + ' ' + Abstract_Connector.escape_columns( queryColumn[j], MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
					}
					querystring += ' ) ';
				}

				querystring += '	) ' + Abstract_Connector.escape_columns( 'ins_union' , MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
				querystring += ' WHERE ';
				querystring += '	NOT EXISTS ( ' + not_exist_select + ' )';
			}
			else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
			{
				querystring += 'INSERT INTO ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function ) + ' (' + Abstract_Connector.expand( query.columns, MSSQL_Connector.escape_column ) + ') VALUES ';

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
				querystring += 'UPDATE ' + Abstract_Connector.escape_columns( query.table + ( query.table_alias ? ' ' + query.table_alias : '' ), MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
			}
			else if( query.operation === 'delete' )
			{
				querystring += 'DELETE FROM ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
			}
			else if( query.operation === 'truncate' )
			{
				querystring += 'TRUNCATE TABLE ' + Abstract_Connector.escape_columns( query.table, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
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
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN (' + subquery + ') ' + query.join[i].table.alias + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ), MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
					}
					else
					{
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ( query.join[i].table_alias ? ' ' + query.join[i].table_alias : '' ) + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ), MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
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
							set += ( i ? ', ' : '' ) + MSSQL_Connector.escape_column(columns[i]) + ' = ' + MSSQL_Connector.escape_value(query.data[0][columns[i]]);
						}
					}

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
					set = Abstract_Connector.escape_columns(query.set, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function);

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
					let condition = Abstract_Connector.escape_columns( query.where[i].condition, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );

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
				let condition = Abstract_Connector.escape_columns( query.group_by.condition, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
				querystring += ' GROUP BY ' + ( query.group_by.data ? Abstract_Connector.expand_values( condition, query.group_by.data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ) : condition );
			}

			if( query.having )
			{
				let condition = Abstract_Connector.escape_columns( query.having.condition, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
				querystring += ' HAVING ' + ( query.having.data ? Abstract_Connector.expand_values( condition, query.having.data, MSSQL_Connector.escape_value, MSSQL_Connector.escape_column ) : condition );
			}

			if( query.order )
			{
				let condition = Abstract_Connector.escape_columns( query.order.condition, MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
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

			if( auto_increment )
			{
				querystring += '; SET IDENTITY_INSERT ' + Abstract_Connector.escape_columns( query.table , MSSQL_Connector.escape_column ) + ' OFF;';
			}

			return querystring;
		};

		this.execute = function( query, callback, indexes = null, auto_increment = null )
		{
			let operation = query.operation;
			if( typeof callback === 'undefined' ){ callback = null; }
			if( typeof query == 'object' )
			{
				query = this.build( query, auto_increment );

				if( operation === 'insert' )
				{
					query += '; ' + Abstract_Connector.escape_columns( 'SELECT SCOPE_IDENTITY() AS id', MSSQL_Connector.escape_column, MSSQL_Connector.transform_function );
				}
			}

			return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
			{
				let start = process.hrtime(); let timeout_ms = remaining_ms;
				let sql_time = 0;

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
					sql_time      : 0,
					query         : query
				};

				emit( 'before-query', query );
				if( ms_connections )
				{
					ms_connections.request().query( query , BIND_IF_FLOW( async (err, rows) =>
					{
						const elapsed_time = process.hrtime(start);
						result.sql_time = elapsed_time[0] * 1000 + elapsed_time[1] / 1000000;

						let elapsed = process.hrtime(start), remaining_ms = timeout_ms - elapsed[0] * 1000 - Math.ceil( elapsed[1] / 1e6 );

						if( err )
						{
							result.ok = false;
							result.error = err;
							result.connector_error = new SQLError( err ).get();
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


							if( rows.hasOwnProperty( 'rowsAffected' ) && Array.isArray( rows.rowsAffected ) && rows.rowsAffected[0] )
							{
								result.affected_rows = rows.rowsAffected[0];
								result.changed_rows = rows.rowsAffected[0];
							}

							result.sql_time +=  sql_time;
							result.affected_rows = result.rows.length;

							if( rows.rowsAffected && Array.isArray( rows.rowsAffected ) && rows.rowsAffected[0] )
							{
								result.changed_rows = result.affected_rows = rows.rowsAffected[0];

								if( operation === 'insert' )
								{
									let new_insertedID = ( rows.recordset && rows.recordset.length ? rows.recordset[0].id : null );

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
							}

							if( rows.rowsAffected && Array.isArray( rows.rowsAffected ) && rows.rowsAffected[0] )
							{
								result.changed_rows = result.affected_rows = rows.rowsAffected[0];
							}
						}

						emit( 'query', result );
						if( callback ) { callback( result ); }
						else { resolve( result ); }
					}), remaining_ms);
				}
				else
				{
					result.ok = false;
					result.error = { code: 'ENOTOPEN' };
					result.connector_error = new SQLError( result.error ).get();

					if( callback ) { callback( result ); }
					else { resolve( result ); }
				}
			});
		};

		this.getTablesQuery = function()
		{
			return 'SELECT Distinct TABLE_NAME FROM information_schema.TABLES';
		};

		this.getColumnsQuery = function( table )
		{
			return 'SELECT a.*, b.is_identity, c.* FROM ( SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N' + this.escape_value( table ) + ' ) a '+
				'LEFT JOIN ( SELECT name, is_identity FROM sys.columns WHERE object_id = object_id(' + this.escape_value( table ) + ') ) b ON b.name = a.COLUMN_NAME ' +
				'LEFT JOIN ( SELECT c.Name table_column, dc.Name cons_name, dc.definition FROM sys.tables t INNER JOIN sys.check_constraints dc ON t.object_id = dc.parent_object_id INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND c.column_id = dc.parent_column_id WHERE t.object_id = object_id(' + this.escape_value( table ) + ') ) c ON c.table_column = a.COLUMN_NAME ';
		};

		this.describe_columns = function( data )
		{
			return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
			{
				let columns = {};

				data.forEach( column => {
					let name = column['COLUMN_NAME'], precision = true;

					columns[ name ] = {};

					if( column['NUMERIC_PRECISION'] === 20 )
					{
						columns[ name ].type = 'BIGINT';
					}
					else if( column['NUMERIC_PRECISION'] === 11 )
					{
						columns[ name ].type = 'INT';
					}
					else if( column['NUMERIC_PRECISION'] === 3 )
					{
						columns[ name ].type = 'TINYINT';
					}
					else
					{
						if( column['CHARACTER_MAXIMUM_LENGTH'] > 100000 )
						{
							columns[ name ].type = 'TEXT';
							precision = false;
						}
						else
						{
							columns[ name ].type = convertDataType[column['DATA_TYPE']];
						}
					}

					if( column.cons_name && column.definition )
					{
						if( [ 'numeric', 'smallint' ].indexOf( column['DATA_TYPE'] ) !== -1 )
						{
							precision = false;
							columns[ name ].type +=  ':UNSIGNED';
						}
						else if( ['nvarchar','varchar'].indexOf( column['DATA_TYPE'] ) !== -1 )
						{
							columns[ name ].type = 'VARCHAR';
							let match = column.definition.match(/'(.*?)'/g );

							if( match && match.length )
							{
								precision = false;

								if( ( match.join( ',' ).replace(/'/g, '').length + 2 ) === column['CHARACTER_MAXIMUM_LENGTH'] )
								{
									columns[ name ].type = 'SET'
								}
								else
								{
									columns[ name ].type = 'ENUM'
								}

								columns[ name ].type +=  ':' + match.join( ',' ).replace(/'/g, '');
							}
						}
					}

					if( column['CHARACTER_MAXIMUM_LENGTH'] && precision )
					{
						columns[ name ].type += ':' + column['CHARACTER_MAXIMUM_LENGTH'];
					}
					else if( column['NUMERIC_PRECISION'] && precision )
					{
						columns[ name ].type += ':' + column['NUMERIC_PRECISION'] + ( column['NUMERIC_SCALE'] ? ','+column['NUMERIC_SCALE'] : '' );
					}

					if( column['IS_NULLABLE'] === 'YES' )
					{
						columns[ name ].null = true;
					}

					if( column['COLUMN_DEFAULT'] )
					{
						let match = column['COLUMN_DEFAULT'] .match(/\((.*?)\)/)
						if( match )
						{
							columns[ name ].default = match[1].replace(/'/g, '');
						}
					}

					if( column['Extra'] === 'on update CURRENT_TIMESTAMP' ) // nepouziva sa
					{
						columns[ name ].update = 'CURRENT_TIMESTAMP';
					}
					else if( column.is_identity )
					{
						columns[ name ].increment = true;
					}
				});

				resolve( columns );
			});
		};

		this.getIndexesQuery = function( table )
		{
			return 'SELECT OBJECT_NAME(ind.object_id) AS ObjectName, ind.name AS IndexName, ind.is_primary_key AS IsPrimaryKey, ind.is_unique AS IsUniqueIndex, col.name AS ColumnName, ic.is_included_column AS IsIncludedColumn, ic.key_ordinal AS ColumnOrder ' +
			'FROM sys.indexes ind ' +
			'INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id ' +
			'INNER JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id ' +
			'INNER JOIN sys.tables t ON ind.object_id = t.object_id ' +
			'WHERE t.is_ms_shipped = 0 AND ind.object_id = object_id(' + this.escape_value( table ) + ') ' +
			'ORDER BY OBJECT_NAME(ind.object_id), ind.is_unique DESC, ind.name, ic.key_ordinal';
		};

		this.describe_indexes = async function( data )
		{
			return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
			{
				let indexes = { primary: {}, unique: {}, index: {} };

				if( data && data.length > 0 )
				{
					data.forEach( index => {
						let type = ( index['IsPrimaryKey'] ? 'primary' : ( index['IsUniqueIndex'] ? 'unique' : 'index' ) );

						if( type === 'primary' ){ index['IndexName'] = 'PRIMARY'; }

						if( !indexes[ type ].hasOwnProperty( index['IndexName'] ) ){ indexes[ type ][ index['IndexName'] ] = []; }

						indexes[ type ][ index['IndexName'] ].push( index['ColumnName'] );
					});

					for( let index in indexes )
					{
						if( indexes[ index ] )
						{
							for( let index_part in indexes[ index ] )
							{
								if( indexes[ index ].hasOwnProperty( index_part ) )
								{
									indexes[ index ][ index_part ] = indexes[ index ][ index_part ].join( ',' );
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

				resolve( indexes );
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
				if( table.indexes.hasOwnProperty(type) && table.indexes[type] && table.indexes[type].length )
				{
					let keys = ( typeof table.indexes[type] === 'string' ? [ table.indexes[type] ] : table.indexes[type] );

					keys.forEach( key => {
						let index = this.create_index( key, type, table.columns, name );

						if( index ){ indexes.push( index ); }
					});
				}
			}

			querystring += ' (' + columns.concat( indexes ).join(',') + ' ) ; END';

			return querystring;
		};

		this.drop_table = function( table )
		{
			return 'IF ( EXISTS ( SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ' + this.escape_value('dbo') + ' AND  TABLE_NAME = ' + this.escape_value( table ) + ')) BEGIN DROP TABLE ' + this.escape_column( table ) + '; END';
		};

		this.create_column = function( data, name, table )
		{
			let column = '';

			if( data )
			{
				if( data.type )
				{
					let type = data.type.split(/[\s,:]+/)[0];

					if( ( [ 'VARCHAR', 'ENUM', 'SET' ].indexOf(type) !== -1 && data.type.toLowerCase().indexOf('multibyte') !== -1 ) || data.multibyte )
					{
						column += ' n' + convertDataType[type];
					}
					else
					{
						column += ' ' + convertDataType[type];
					}

					let size = data.type.match(/:([0-9]+)/);

					if( type === 'DECIMAL' )
					{
						column += '('+data.type.match(/:([0-9,]+)/)[1]+')';
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
						let max_length = 0;
						data.type.split(':')[1].trim().split(/\s*,\s*/).forEach( value => max_length = Math.max( max_length, value.length ) );

						column += '(' + ( max_length < 10 ? ( ( max_length === 9 ? max_length + 1 : max_length + 2 ) ) : max_length ) + ')';
					}
					else if( type === 'SET' )
					{
						column += '(' + ( data.type.split(':')[1].trim().length + 2 ) + ')';
					}
					else if( size )
					{
						column += '(' + size[1] + ')';
					}

					if( ( [ 'VARCHAR' ].indexOf(type) !== -1 && data.type.toLowerCase().indexOf('multibyte') !== -1 ) || data.multibyte )
					{

					}

					column += ( data.null ? ' NULL' : ' NOT NULL'  );

					if( typeof data.default !== 'undefined' )
					{
						if( type === 'DECIMAL' )
						{
							column += ' DEFAULT ' + ( ['CURRENT_TIMESTAMP', 'NULL'].indexOf(data.default) === -1 ? this.escape_value( parseFloat( data.default ).toString() ) : parseFloat( data.default ) );
						}
						else
						{
							column += ' DEFAULT ' + ( ['CURRENT_TIMESTAMP', 'NULL'].indexOf(data.default) === -1 ? this.escape_value( data.default.toString() ) : data.default );
						}
					}

					if( data.update )
					{
						//	column += ' ON UPDATE ' + ( ['CURRENT_TIMESTAMP'].indexOf(columnData['update']) === -1 ? this.escape_value( columnData['update'] ) : columnData['update'] );
					}

					if( data.increment )
					{
						column += ' IDENTITY(1,1)';
					}

					if( ['SET', 'ENUM'].indexOf( type ) > -1 )
					{
						column += ' CONSTRAINT ' + this.generate_index_name( name, table, null, 'cons' ) + ' CHECK ( ' + this.escape_column(name) + ' IN (' + Abstract_Connector.expand( data.type.split(':')[1].trim().split(/\s*,\s*/), this.escape_value ) + ') )';
					}

					if( ( [ 'INT', 'BIGINT', 'TINYINT' ].indexOf(type) !== -1 && data.type.toUpperCase().indexOf(':UNSIGNED') !== -1 ) || data.unsigned  )
					{
						column += ' CONSTRAINT ' + this.generate_index_name( name, table, null, 'uns' ) + ' check ( ' + this.escape_column(name) + ' >= 0 )';
					}
				}
			}

			return column;
		};

		this.create_database_query = async function( database, option = [] )
		{
			let queries = [];

			queries.push( 'IF NOT EXISTS ( SELECT * FROM sys.databses WHERE name = '+this.escape_value(database)+' ) CREATE DATABASE IF NOT EXISTS ' + this.escape_column(database) + ' COLLATE utf8_general_ci;' );
			queries.push( 'USE ' + this.escape_column(database) + ';' );

			return queries.join(' ');
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

		this.drop_column_query = function( column )
		{
			return '   DROP ' + this.escape_column( column );
		}

		this.modify_column = function( column, modiefiedColumnData, previousColumn, setValues )
		{
			let alter = [], update = [], afterUpdate = [];
			alter.push( '   ADD ' + this.escape_column( 'tmp_' + column ) + modiefiedColumnData );
			update.push( this.escape_column( 'tmp_' + column ) + ' = ' + setValues );
			afterUpdate.push( '   DROP ' + this.escape_column( column ) );
			afterUpdate.push( '   ALTER COLUMN ' + this.escape_column( 'tmp_' + column ) + ' ' + this.escape_column( column ) + ' ' + modiefiedColumnData );
			return { alterColumns: alter, updateValues: update, afterUpdateColumns: afterUpdate };
		};

		this.create_index = function( index, type, columns, table, alter )
		{
			if( !alter ){ alter = false; }

			let cols = index.split(/\s*,\s*/);
			let sql = ( alter ? 'ADD ' : ' ' ) + ( ['primary', 'unique'].indexOf( type ) === -1 ? 'INDEX ' : ' CONSTRAINT ' ) + this.generate_index_name( cols.join(','), table, ( type === 'primary' ? 'PK' : ( type === 'unique' ? 'UC' : 'C' )  ), null ) + ' ' + ( type === 'primary' ? 'PRIMARY KEY ' : ( type === 'unique' ? ( alter ? 'UNIQUE ' : 'UNIQUE ' ) : ( type === 'index' ? ( alter ? 'INDEX ' : ' ' ) : '' ) ) ) + ' (';

			cols.forEach( column => {
				sql += this.escape_column( column );

				if( columns[column] && ['VARCHAR', 'TEXT'].indexOf( columns[column].type.split(/[\s,:]+/)[0] ) > -1 )
				{
					let max_length = 255, length = 256;

					let match = columns[column].type.match(/:([0-9]+)/);
					if( match )
					{
						//	length = Math.min(length, parseInt(match[1]));
					}
				}

				sql += ( ( cols.indexOf(column) < cols.length - 1 ) ? ',' : '' );
			});

			sql += ')';

			return sql;
		};

		this.generate_index_name = function( columns, table, prefix, type )
		{
			let constraintName = [];

			if( prefix ){ constraintName.push( prefix ); }
			if( table ){ constraintName.push( table ); }

			if( typeof columns === 'string' ){ columns = columns.split( ',' ); }

			if( Array.isArray( columns ) )
			{
				columns.forEach( column => {
					if( column.length > 4 && columns.length > 2 )
					{
						let joinedColumn = '';
						column.split( '_' ).forEach( col => { if( col ){ joinedColumn += splitedColumn[n].substr( 0, 4 ).toUpperCase(); }});

						constraintName.push( joinedColumn );
					}
					else { constraintName.push( column ); }
				});

				if( type ) { constraintName.push( type ); }

				usedKeyName.push( constraintName.join('_') );
				return constraintName.join('_');
			}
			else { return ''; }
		};

		this.drop_index = function( indexes, type )
		{
			let drop = [];
			if( type === 'primary' )
			{
				drop.push( 'DROP PRIMARY KEY' );
			}
			else if( typeof indexes === 'string' )
			{
				drop.push( 'DROP ' + ( type === 'unique' ? 'INDEX ' : 'INDEX ' ) + this.escape_column( this.generate_index_name( indexes ) ) );
			}
			else if( indexes.length )
			{
				indexes.forEach( index => drop.push( 'DROP ' + ( type === 'unique' ? 'INDEX ' : 'INDEX ' ) + this.escape_column( index ) ) );
			}

			return drop.join( ', ' );
		}

		this.connected = function()
		{
			return ms_connected;
		};

		connect();

	})();
};
