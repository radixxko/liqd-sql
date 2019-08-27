'use strict';

var Abstract_Connector = require( './abstract.js')({
	escape_keyword: '\\'
});
const TimedPromise = require('liqd-timed-promise');
const SQLError = require( '../errors.js');

const BIND_IF_FLOW = callback => typeof LIQD_FLOW !== 'undefined' && LIQD_FLOW.started ? LIQD_FLOW.bind( callback ) : callback;

function getMiliseconds()
{
	return (new Date()).getTime();
}

module.exports = function( config, emit )
{
	return new( function()
	{
		const MYSQL_Connector = this;

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

			if( typeof options.default_charset != 'undefined' ){ default_charset = options.default_charset; }
			if( config.non_escape_keywords && Array.isArray( config.non_escape_keywords ) && config.non_escape_keywords.length ){ Abstract_Connector.abstract_keywords = Abstract_Connector.abstract_keywords.concat( config.non_escape_keywords ); }

			options.dateStrings        = 'date';
			options.supportBigNumbers  = true;

			try
			{
				my_sql = require( 'mysql');
			}
			catch(e){ throw new Error('Please install "mysql" module to use mysql connector'); }

			my_connections = my_sql.createPool( options );

			const connectedConnections = new Set();
			const connectHandler = ( conn ) =>
			{
				connectedConnections.add( conn );

				if( !my_connected )
				{
					my_connected = true;
					emit( 'status', 'connected' );
				}
			}
			const disconnectHandler = ( conn ) =>
			{
				connectedConnections.delete( conn );

				if( my_connected && connectedConnections.size === 0 )
				{
					my_connected = false;
					emit( 'status', 'disconnected' );
				}
			}

			my_connections.on( 'connection', (conn) =>
			{
				connectHandler( conn );

				conn.once( 'end', disconnectHandler.bind( null, conn ) );
			});
		}

		function aggregate_columns( columns, aggregator = 'MAX' )
		{
			let columnRE = /^((`[^`]+`\.)*(`[^`]+`))$/m;
			let columnsRE = /^(\s*((`[^`]+`\.)*(`[^`]+`))\s*,)+\s*((`[^`]+`\.)*(`[^`]+`))\s*$/m;
			let aggregationRE = /((AVG|BIT_AND|BIT_OR|BIT_XOR|CHECKSUM_AGG|COUNT|COUNT_BIG|GROUP_CONCAT|GROUPING|GROUPING_ID|JSON_ARRAYAGG|JSON_OBJECTAGG|MAX|MIN|STD|STDDEV|STDDEV_POP|STDDEV_SAMP|STRING_AGG|SUM|VAR|VARP|VAR_POP|VAR_SAMP|VARIANCE)\s*\((\s*DISTINCT(\s*\(|\s+){0,1}){0,1}|([\)0-9'"`]{0,1}))\s*?((`[^`]+`\.)*`[^`]+`)/gm;
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

		this.ping_query = function()
		{
			my_connections.query('SELECT \'connected\' FROM DUAL', () => {});
		}

		this.unescape_column = function( column )
		{
			if(column)
			{
				return column.replace(/`/gi, '' );
			}
			else return '';
		};

		this.escape_column = function( column )
		{
			if(column)
			{
				return '`' + column + '`';
			}
			else return '';
		};

		this.transform_function = function( transform, position )
		{
			if( transform.hasOwnProperty( position ) && typeof transform[position] === 'object' && transform[position].name )
			{
				if( transform[position].name.toUpperCase() === 'IIF' )
				{
					let label = 'IF( '+ transform[position].values.join(', ') +')'; //todo kontrola na pocet

					return label;
				}
				else if( transform[position].name.toUpperCase() === 'GETUTCDATE' )
				{
					return 'NOW()';
				}
				else { return null; }
			}
			else { return null; }
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

		this.build = function( query )
		{
			let querystring = '', use_database = true;

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
						for (let i = 0; i < union.length; i++)
						{
							if( typeof union[i] === 'object' )
							{
								let dual_data = [];

								if( used_columns.length )
								{
									for( let p = 0; p < used_columns.length; p++ )
									{
										dual_data.push( ( union[i].hasOwnProperty( used_columns[p] ) ? MYSQL_Connector.escape_value( union[i][ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  );
									}
								}
								else
								{
									for( let column in union[i] )
									{
										if( union[i].hasOwnProperty( column ) ){ dual_data.push( MYSQL_Connector.escape_value( union[i][ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  ); }
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
								dual_data.push( ( union.hasOwnProperty( used_columns[p] ) ? MYSQL_Connector.escape_value( union[ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  );
							}
						}
						else
						{
							for( let column in union )
							{
								if( union.hasOwnProperty( column ) ){ dual_data.push( MYSQL_Connector.escape_value( union[ column ] ) + ' ' + Abstract_Connector.escape_columns( column, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  ); }
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
				let escaped_columns = Abstract_Connector.escape_columns( Abstract_Connector.expand_values( query.columns.columns, query.columns.data, MYSQL_Connector.escape_value ), MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				if( query.group_by || query.having )
				{
					escaped_columns = aggregate_columns( escaped_columns, 'MAX' );
				}

				querystring += 'SELECT ' + escaped_columns + ( ( query.table && query.table != 'TEMPORARY' ) ? ' FROM ' + Abstract_Connector.escape_columns( ( !/\s*\(.*\)\s*/.test( query.table ) && query.database ? query.database + '.' : '' ) + query.table + ( query.table_alias ? ' ' + query.table_alias : '' ), MYSQL_Connector.escape_column, null ) : '' );
			}
			else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
			{
				querystring += 'INSERT '+( query.options && query.options.indexOf('ignore') !== -1  ? 'IGNORE' : '' )+' INTO ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function ) + ' (' + Abstract_Connector.expand( query.columns, MYSQL_Connector.escape_column ) + ') VALUES ';

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
				querystring += 'UPDATE ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table + ( query.table_alias ? ' ' + query.table_alias : '' ), MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
			}
			else if( query.operation === 'delete' )
			{
				querystring += 'DELETE FROM ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
			}
			else if( query.operation === 'truncate' )
			{
				querystring += 'TRUNCATE TABLE ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
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
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN (' + subquery + ') ' + query.join[i].table.alias + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ), MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
					}
					else
					{
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ( query.join[i].table_alias ? ' ' + query.join[i].table_alias : '' ) + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ), MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
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
					set = Abstract_Connector.escape_columns(query.set, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

					if(query.data !== null)
					{
						set = Abstract_Connector.expand_values(set, query.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column);
					}
				}

				querystring += ' SET ' + set;
			}

			if( query.where && query.operation !== 'insert' )
			{
				let where = '';

				for( let i = 0; i < query.where.length; ++i )
				{
					let condition = Abstract_Connector.escape_columns( query.where[i].condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

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

			if( query.group_by && query.operation === 'select' )
			{
				let condition = Abstract_Connector.escape_columns( query.group_by.condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				querystring += ' GROUP BY ' + ( query.group_by.data ? Abstract_Connector.expand_values( condition, query.group_by.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ) : condition );
			}

			if( query.having && query.operation === 'select' )
			{
				let condition = Abstract_Connector.escape_columns( query.having.condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				querystring += ' HAVING ' + ( query.having.data ? Abstract_Connector.expand_values( condition, query.having.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ) : condition );
			}

			if( query.order && query.operation !== 'insert' )
			{
				let condition = Abstract_Connector.escape_columns( query.order.condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				querystring += ' ORDER BY ' + ( query.order.data ? Abstract_Connector.expand_values( condition, query.order.data, MYSQL_Connector.escape_value, MYSQL_Connector.escape_column ) : condition );
			}

			if( query.limit && query.operation !== 'insert'  )
			{
				querystring += ' LIMIT ' + query.limit;
			}

			if( query.offset && query.operation !== 'insert' )
			{
				if( !query.limit ) { querystring += ' LIMIT 18446744073709551615'; }
				querystring += ' OFFSET ' + query.offset;
			}

			querystring = querystring.replace( '`#AI_CI`', '#AI_CI' );

			return querystring;
		};

		this.build_wo_values = function( query )
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
						for (let i = 0; i < union.length; i++)
						{
							if( typeof union[i] === 'object' )
							{
								let dual_data = [];

								if( used_columns.length )
								{
									for( let p = 0; p < used_columns.length; p++ )
									{
										dual_data.push( ( union[i].hasOwnProperty( used_columns[p] ) ? ':value' : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  );
									}
								}
								else
								{
									for( let column in union[i] )
									{
										if( union[i].hasOwnProperty( column ) ){ dual_data.push( ':value ' + Abstract_Connector.escape_columns( column, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  ); }
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
								dual_data.push( ( union.hasOwnProperty( used_columns[p] ) ? ':value' : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  );
							}
						}
						else
						{
							for( let column in union )
							{
								if( union.hasOwnProperty( column ) ){ dual_data.push( ':value ' + Abstract_Connector.escape_columns( column, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function )  ); }
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
				let escaped_columns = Abstract_Connector.escape_columns( query.columns.columns, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
				querystring += 'SELECT ' + escaped_columns + ( ( query.table && query.table != 'TEMPORARY' ) ? ' FROM ' + Abstract_Connector.escape_columns( ( !/\s*\(.*\)\s*/.test( query.table ) && query.database ? query.database + '.' : '' ) + query.table + ( query.table_alias ? ' ' + query.table_alias : '' ), MYSQL_Connector.escape_column, null ) : '' );
			}
			else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
			{
				querystring += 'INSERT '+( query.options && query.options.indexOf('ignore') !== -1  ? 'IGNORE' : '' )+' INTO ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function ) + ' (' + Abstract_Connector.expand( query.columns, MYSQL_Connector.escape_column ) + ') VALUES :values';

				//for( let i = 0; i < query.data.length; ++i )
			}
			else if( query.operation === 'update' )
			{
				querystring += 'UPDATE ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table + ( query.table_alias ? ' ' + query.table_alias : '' ), MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
			}
			else if( query.operation === 'delete' )
			{
				querystring += 'DELETE FROM ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
			}
			else if( query.operation === 'truncate' )
			{
				querystring += 'TRUNCATE TABLE ' + Abstract_Connector.escape_columns( ( query.database ? query.database + '.' : '' ) + query.table, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
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
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN (' + subquery + ') ' + query.join[i].table.alias + ' ON ' + query.join[i].condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
					}
					else
					{
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ( query.join[i].table_alias ? ' ' + query.join[i].table_alias : '' ) + ' ON ' + query.join[i].condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
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
							set += ( i ? ', ' : '' ) + MYSQL_Connector.escape_column(columns[i]) + ' = :value';
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
						set += ( i ? ', ' : '' ) + MYSQL_Connector.escape_column(columns[i]) + ' = :value';
					}
				}
				else
				{
					set = Abstract_Connector.escape_columns(query.set, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );
				}

				querystring += ' SET ' + set;
			}

			if( query.where && query.operation !== 'insert')
			{
				let where = '';

				for( let i = 0; i < query.where.length; ++i )
				{
					let condition = Abstract_Connector.escape_columns( query.where[i].condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

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

			if( query.group_by && query.operation === 'select')
			{
				let condition = Abstract_Connector.escape_columns( query.group_by.condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				querystring += ' GROUP BY ' + condition;
			}

			if( query.having && query.operation === 'select')
			{
				let condition = Abstract_Connector.escape_columns( query.having.condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				querystring += ' HAVING ' + condition;
			}

			if( query.order && query.operation !== 'insert' )
			{
				let condition = Abstract_Connector.escape_columns( query.order.condition, MYSQL_Connector.escape_column, MYSQL_Connector.transform_function );

				querystring += ' ORDER BY ' + condition;
			}

			if( query.limit && query.operation !== 'insert' )
			{
				querystring += ' LIMIT ' + query.limit;
			}

			if( query.offset && query.operation !== 'insert' )
			{
				if( !query.limit ) { querystring += ' LIMIT 18446744073709551615'; }
				querystring += ' OFFSET ' + query.offset;
			}

			querystring = querystring.replace( '`#AI_CI`', '#AI_CI' );

			return querystring;
		};

		this.execute = function( query, callback, indexes = null, auto_increment = null )
		{
			let generalized_query = '';

			if( typeof callback === 'undefined' ){ callback = null; }
			if( typeof query == 'object' )
			{
				generalized_query = this.build_wo_values( query );
				query = this.build( query );
			}

			return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
			{
				let start = process.hrtime(); let timeout_ms = remaining_ms;
				let sql_time = 0;

				emit( 'before-query', query );
				my_connections.query( query, BIND_IF_FLOW( async ( err, rows ) =>
				{
					const elapsed_time = process.hrtime(start);
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
						sql_time      : elapsed_time[0] * 1000 + elapsed_time[1] / 1000000,
						query         : query,
						generalized_query : generalized_query
					};

					emit( 'generalized-query', generalized_query );

					let elapsed = process.hrtime(start), remaining_ms = timeout_ms - elapsed[0] * 1000 - Math.ceil( elapsed[1] / 1e6 );

					if( err )
					{
						result.ok = false;
						result.error = err;
						result.connector_error = new SQLError( err ).get();
					}
					else
					{
						if( rows.length )
						{
							for( let i = 0; i < rows.length; ++i )
							{
								result.rows.push( rows[i] );
							}

							result.row = result.rows[0];
						}

						result.sql_time +=  sql_time;
						result.affected_rows = rows.affectedRows || result.rows.length;


						if( rows.affectedRows && rows.insertId )
						{
							if( auto_increment )
							{
								for( let i = 0; i < rows.affectedRows; ++i )
								{
									result.inserted_ids.push( rows.insertId - rows.affectedRows + 1 + i );
									result.changed_ids.push( rows.insertId - rows.affectedRows + 1 + i );
								}
							}
							else
							{
								for( let i = 0; i < rows.affectedRows; ++i )
								{
									result.inserted_ids.push( rows.insertId  + i );
									result.changed_ids.push( rows.insertId + i );
								}

								result.inserted_id = result.changed_id = result.inserted_ids[0];
							}

							result.inserted_id = result.changed_id = result.inserted_ids[0];
						}

						result.changed_rows = rows.changedRows || result.changed_ids.length ;
					}

					emit( 'query', result );
					if( callback ) { callback( result ); }
					else { resolve( result ); }
				}), remaining_ms);

			});
		};

		this.getTablesQuery = function()
		{
			return 'SHOW TABLES';
		};

		this.getColumnsQuery = function( table )
		{
			return 'SHOW FULL COLUMNS FROM ' + this.escape_column( table );
		};

		this.describe_columns = function( data )
		{
			return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
			{
				let columns = {};

				data.forEach( column => {
					let name = column.Field;

					columns[ name ] = {};
					columns[ name ].type = column['Type'].split(/[\s,:(]+/)[0].toUpperCase();

					let match = column['Type'].match(/\((.*?)\)/)
					if( match ){ columns[ name ].type += ':' + match[1].replace(/'/g, ''); }

					if( column['Type'].indexOf( 'unsigned' ) !== - 1) { columns[ name ].unsigned = true; }

					if( column['Null'] === 'YES' ) { columns[ name ].null = true; }

					if( column['Default'] || column['Default'] === '' )
					{
						columns[ name ].default = column['Default'];
					}
					else if( column['Null'] === 'YES' && column['Default'] === null )
					{
						columns[ name ].default = 'NULL';
					}

					if( column['Extra'] === 'on update CURRENT_TIMESTAMP' )
					{
						columns[ name ].update = 'CURRENT_TIMESTAMP';
					}
					else if( column['Extra'] === 'auto_increment' )
					{
						columns[ name ].increment = true;
					}

					if( column['Collation'] !== default_charset )
					{
						columns[ name ].multibyte = column['Collation'];
					}
				});

				resolve( columns );
			});
		};

		this.getIndexesQuery = function( table )
		{
			return 'SHOW INDEX FROM ' + this.escape_column( table );
		};

		this.describe_indexes = function( data )
		{
			return new TimedPromise( async ( resolve, reject, remaining_ms ) =>
			{
				let indexes = { primary: {}, unique: {}, index: {} };

				if( data && data.length > 0 )
				{
					data.forEach( index => {
						let type = ( index.Key_name === 'PRIMARY' ? 'primary' : ( index.Non_unique === 0 ? 'unique' : 'index' ) );

						if( !indexes[ type ].hasOwnProperty( index.Key_name ) ){ indexes[ type ][ index.Key_name ] = []; }

						indexes[ type ][ index.Key_name ].push( index.Column_name );
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

		this.create_table = function( table, name = null, database = null, options = [] )
		{
			let columns = [], indexes = [];
			let querystring = 'CREATE TABLE ' + ( options.includes('unsafe') ? 'IF NOT EXISTS ' : '' ) + Abstract_Connector.escape_columns( ( database ? database + '.' : '' ) + name, MYSQL_Connector.escape_column, null );

			for( let column in table.columns )
			{
				if( table.columns.hasOwnProperty( column ) )
				{
					columns.push( ' ' + this.escape_column( column ) + ' ' + this.create_column( table.columns[ column ] ) );
				}
			}

			for( let type in table.indexes )
			{
				if( table.indexes.hasOwnProperty(type) && table.indexes[type] && table.indexes[type].length )
				{
					let keys = ( typeof table.indexes[type] === 'string' ? [ table.indexes[type] ] : table.indexes[type] );

					keys.forEach( key => {
						let alterTableIndexes = this.create_index( key, type, table.columns );

						if( alterTableIndexes ){ indexes.push( alterTableIndexes ); }
					});
				}
			}

			querystring += ' (' + columns.concat( indexes ).join(',') + ' ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_general_ci';

			return querystring;
		}

		this.drop_table = function( table, database = null, options = [] )
		{
			let querystring = 'DROP TABLE ' + ( options.includes('unsafe') ? 'IF EXISTS ' : '' ) + Abstract_Connector.escape_columns( ( database ? database + '.' : '' ) + table, MYSQL_Connector.escape_column, null );
			return querystring;
		}

		this.create_column = function( columnData )
		{
			let column = '';

			if( columnData )
			{
				if( columnData.type )
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

		this.create_database_query = function( database, options = [] )
		{
			let queries = [];

			queries.push( 'CREATE DATABASE ' + ( options.includes('unsafe') ? 'IF NOT EXISTS ' : '' ) + this.escape_column(database) + ' DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;' );
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
			alter.push( '   ADD ' + this.escape_column( 'tmp_' + column ) + modiefiedColumnData + ( previousColumn !== '' ? ' AFTER ' + this.escape_column( previousColumn ) : '' ) );
			update.push( this.escape_column( 'tmp_' + column ) + ' = ' + setValues );
			afterUpdate.push( '   DROP ' + this.escape_column( column ) );
			afterUpdate.push( '   CHANGE ' + this.escape_column( 'tmp_' + column ) + ' ' + this.escape_column( column ) + ' ' + modiefiedColumnData + ( previousColumn !== '' ? ' AFTER ' + this.escape_column( previousColumn ) : '' ) );
			return { alterColumns: alter, updateValues: update, afterUpdateColumns: afterUpdate };
		}

		this.create_index = function( index, type, columns, alter )
		{
			if( !alter ){ alter = false; }

			let cols = index.split(/\s*,\s*/);
			let sql =  ( alter ? 'ADD ' : ' ' ) + ' ' + ( type === 'primary' ? 'PRIMARY KEY ' : ( type === 'unique' ? ( alter ? 'UNIQUE INDEX ' : 'UNIQUE KEY ' ) : ( type === 'index' ? ( alter ? 'INDEX ' : 'KEY ' ) : '' ) ) ) + ( type !== 'primary' ? this.escape_column( this.generate_index_name( cols.join('_') ) ) : '' ) + ' (';

			cols.forEach( column => {
				sql += this.escape_column( column );

				if( columns[column] && ['VARCHAR', 'TEXT', 'LONGTEXT'].indexOf( columns[column].type.split(/[\s,:]+/)[0] ) > -1 )
				{
					let max_length = 255, length = 256;

					let match = columns[column].type.match(/:([0-9]+)/);
					if( match )
					{
						length = Math.min(length, parseInt(match[1]));
					}

					if( length > max_length ) { sql += '(' + max_length + ')'; }
				}

				sql += ( ( cols.indexOf(column) < cols.length - 1 ) ? ',' : '' );
			});

			sql += ')';

			return sql;
		}

		this.generate_index_name = function( columns, table, prefix, type )
		{
			return ( typeof columns === 'string' ? columns.split( ',' ).join('_') : columns.join('_') );
		}

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
				indexes.forEach( index => drop.push( 'DROP ' + ( type === 'unique' ? 'INDEX ' : 'INDEX ' ) + this.escape_column( index )));
			}

			return drop.join( ', ' );
		}

		this.connected = function()
		{
			return my_connected;
		}

		connect();

	})();


};
