'use strict';

const Abstract_Connector = require( './abstract.js');
const TimedPromise = require('liqd-timed-promise');
const SQLError = require( '../errors.js');

const MAX_SAFE_DECIMALS = Math.ceil(Math.log10(Number.MAX_SAFE_INTEGER));
const MAX_UINT = '18446744073709551616';
const MIN_UINT = Number.MAX_SAFE_INTEGER.toString();

const BIND_IF_FLOW = callback => typeof LIQD_FLOW !== 'undefined' && LIQD_FLOW.started ? LIQD_FLOW.bind( callback ) : callback;
let collumn_prefix = 'A__';

function getMiliseconds()
{
	return (new Date()).getTime();
}

function capitalize( word )
{
	return word.charAt(0).toUpperCase()+word.substr(1);
}

module.exports = function( config, emit )
{
	return new( function()
	{
		const ORACLE_Connector = this;

		let convertDataType =
		{
			'VARCHAR'   :   'VARCHAR2',
			'TINYINT'   :   'NUMBER',
			'INT'       :   'NUMBER',
			'BIGINT'    :   'NUMBER',
			'DECIMAL'   :   'NUMBER',
			'TIMESTAMP' :   'TIMESTAMP',
			'ENUM'      :   'VARCHAR2',
			'SET'       :   'VARCHAR2',
			'TEXT'      :   'CLOB',
			'LONGTEXT'  :   'CLOB',
		};

		var usedKeyName = [];

		var oracle_sql = null,
		oracle_connections = null,
		oracle_connected = false;

		this.ping_query = function()
		{
			if( oracle_connections )
			{
				oracle_connections.getConnection( async (err, conn) =>
				{
					if(conn){
						await conn.execute( 'SELECT 1 connected FROM DUAL ', {}, { outFormat: oracle_sql.OBJECT, autoCommit: true } , BIND_IF_FLOW( async (err, rows) =>
						{
							conn.release( function(err){});

							if( err )
							{
								if( oracle_connected )
								{
									oracle_connected = false;
									emit( 'status', 'disconnected' );
								}
							}
							else
							{
								if( !oracle_connected )
								{
									oracle_connected = true;
									emit( 'status', 'connected' );
								}
							}
						}));
					}
					else
					{
						if( oracle_connected )
						{
							oracle_connected = false;
							emit( 'status', 'disconnected' );
						}
					}
				});
			}
		}

		async function connect()
		{
			let options = JSON.parse(JSON.stringify( config ));

			try
			{
				oracle_sql = require( 'oracledb');
			}
			catch(e){ throw new Error('Please install "oracledb" module to use oracle connector'); }

			oracle_sql.fetchAsString = [ oracle_sql.CLOB, oracle_sql.NUMBER ];
			oracle_connections = await oracle_sql.createPool( options );

			if( oracle_connections )
			{
				oracle_connected = true;
				emit( 'status', 'connected' );
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
				if( transform[position].name.toUpperCase() === 'CONCAT' )
				{
					let label = '', value_array = [];
					if( transform[position].values.length )
					{
						transform[position].values.forEach( value => {
							value_array.push( value );
							if( value_array.length === 2 )
							{
								label = ' CONCAT(' + value_array.join(', ') + ')';
								value_array = [ label ];
							}
						});

						label = label.replace(/(DISTINCT\s+)/i, 'asdadadad' );
					}

					return label;
				}
				else if( transform[position].name.toUpperCase() === 'RADIANS' )
				{
					return '0.01745329252 * ( '+transform[position].values[0]+' )';
				}
				else if( transform[position].name.toUpperCase() === 'DEGREES' )
				{
					return '57.2957795131 * ( '+transform[position].values[0]+' )';
				}
				else if( transform[position].name.toUpperCase() === 'UNIX_TIMESTAMP' )
				{
					if( transform[position].values.length )
					{
						return 'UNIX_TIMESTAMP( '+transform[position].values[0]+' )'
					}
					else{ return 'UNIX_TIMESTAMP( systimestamp )' }
				}
				else if( transform[position].name.toUpperCase() === 'IF' )
				{
					if( transform.hasOwnProperty( position - 1 ) && typeof transform[ position - 1 ] === 'object' && transform[ position - 1 ].name === transform[position].name )
					{
						if( transform[ position - 1 ].values.length === 1 )
						{
							let new_values = [ transform[position].values[1] , transform[ position - 1 ].values[0], transform[position].values[2] ];
							transform[ position - 1 ].values[0] = transform[ position - 1 ].values[0] + ' AND ' + transform[position].values[0];

							transform[position].values = new_values;
							return transform[position].values;
						}
						else { return transform[position].values; }
					}
					else
					{
						let label = ' ( CASE';

						for( let i = 0; i < transform[position].values.length; i = i+2 )
						{
							if( transform[position].values[ i+1 ] ){ label += ' WHEN '+transform[position].values[ i ]+' THEN '+ transform[position].values[ i+1 ];  }
							else { label += ' ELSE '+ transform[position].values[ i ]; }
						}

						return label + ' END )'
					}
				}
				else { return null; }
			}
			else { return null; }
		};

		this.escape_value = function( value )
		{
			if( value === '' ){ return 'NULL'; }

			if( !isNaN(value) && value && ( ( !isNaN(value) && typeof value === 'string' && value.indexOf( '.' ) !== -1 ) || value.length < MIN_UINT.length || ( value.length == MIN_UINT.length && value <= MIN_UINT ) ) )
			{
				let trimmed = value.trim();
				if( trimmed  ){ value = parseFloat( value ); }
			}

			if( typeof value === 'string' )
			{
				if( value.match(/^\d+$/) && ( value.length > MIN_UINT.length || ( value.length == MIN_UINT.length && value > MIN_UINT ) ) && ( value.length < MAX_UINT.length || ( value.length == MAX_UINT.length && value <= MAX_UINT ) ) )
				{
					return value;
				}

				value = value.replace(/\\\\/g, '\\');
				if( value.indexOf( 'NOW()' ) !== -1 ) //todo nejak doriesit
				{
					value = value.replace('NOW()', 'CURRENT_TIMESTAMP' );

					if( value.indexOf( 'INTERVAL' ) !== -1 )
					{
						let [ , operator, to_replace, duration, interval ] = value.match(/([+-]+)\s*(INTERVAL\s+(\d*?)\s+([a-z]+))/i);
						value = value.replace(to_replace, 'INTERVAL \'' + duration + '\' ' + interval + '' );
					}

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
				return 'NULL';
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
				return '\''+ value.toString().replace(/'/g, '\'\'') + '\'';
			}
		};

		this.escape_value_conn = function( value )
		{
			if( !isNaN(value) && value && ( ( !isNaN(value) && typeof value === 'string' && value.indexOf( '.' ) !== -1 ) || value.length < MIN_UINT.length || ( value.length == MIN_UINT.length && value <= MIN_UINT ) ) )
			{
				let trimmed = value.trim();
				if( trimmed  ){ value = parseFloat( value ); }

			}

			if( typeof value === 'string' )
			{
				if( value.match(/^\d+$/) && ( value.length > MIN_UINT.length || ( value.length == MIN_UINT.length && value > MIN_UINT ) ) && ( value.length < MAX_UINT.length || ( value.length == MAX_UINT.length && value <= MAX_UINT ) ) )
				{
					return value;
				}

				value = value.replace(/\\\\/g, '\\');
				if( value.indexOf( 'NOW()' ) !== -1 ) //todo nejak doriesit
				{
					value = value.replace('NOW()', 'CURRENT_TIMESTAMP' );

					if( value.indexOf( 'INTERVAL' ) !== -1 )
					{
						let [ , operator, to_replace, duration, interval ] = value.match(/([+-]+)\s*(INTERVAL\s+(\d*?)\s+([a-z]+))/i);
						value = value.replace(to_replace, 'INTERVAL \'' + duration + '\' ' + interval + '' );
					}

					return value;
				}
				else
				{
					if (value.indexOf('&__escaped__:') === 0) {
						return value.substr('&__escaped__:'.length);
					}

					return '' + value.replace(/'/g, '\'\'') + '';
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
				return '' + value.toString('hex') + '';  //TODO
			}
			else if( typeof value === 'object' )
			{
				return '' + JSON.stringify( value ) + '';
			}
			else
			{
				return ''+ value.toString().replace(/'/g, '\'\'') + '';
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
										dual_data.push( ( union[i].hasOwnProperty( used_columns[p] ) ? ORACLE_Connector.escape_value( union[i][ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], ORACLE_Connector.escape_column, ORACLE_Connector.transform_function)  );
									}
								}
								else
								{
									for( let column in union[i] )
									{
										if( union[i].hasOwnProperty( column ) ){ dual_data.push( ORACLE_Connector.escape_value( union[i][ column ] ) + ' ' + Abstract_Connector.escape_columns( column, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function)  ); }
									}
								}

								select_dual.push( 'SELECT ' + dual_data.join( ', ' ) + ' FROM DUAL'  );
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
								dual_data.push( ( union.hasOwnProperty( used_columns[p] ) ? ORACLE_Connector.escape_value( union[ used_columns[ p ] ] ) : 'NULL' ) + ' ' + Abstract_Connector.escape_columns( used_columns[p], ORACLE_Connector.escape_column, ORACLE_Connector.transform_function)  );
							}
						}
						else
						{
							for( let column in union )
							{
								if( union.hasOwnProperty( column ) ){ dual_data.push( ORACLE_Connector.escape_value( union[ column ] ) + ' ' + Abstract_Connector.escape_columns( column, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function)  ); }
							}
						}

						unions.push( 'SELECT ' + dual_data.join( ', ' ) + ' FROM DUAL' );
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
			//	querystring += '; SET IDENTITY_INSERT ' + Abstract_Connector.escape_columns( query.table , ORACLE_Connector.escape_column ) + ' ON;';
			}

			if( query.operation === 'select' )
			{
				let escaped_columns = Abstract_Connector.escape_columns( Abstract_Connector.expand_values( query.columns.columns, query.columns.data, ORACLE_Connector.escape_value ), ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );

				if( query.group_by || query.having )
				{
					escaped_columns = aggregate_columns( escaped_columns, 'MAX' );
				}

				querystring += 'SELECT '
				querystring += escaped_columns + ( ( query.table && !['DUAL','TEMPORARY'].includes(query.table) ) ? ' FROM ' + Abstract_Connector.escape_columns( query.table, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function ) : '' );
			}
			else if( query.operation === 'insert' && query.options && query.options.indexOf('ignore') !== -1 )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
			{
				let queryColumn = query.columns;
				querystring += 'INSERT INTO ' + Abstract_Connector.escape_columns( query.table, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function ) + ' (' + Abstract_Connector.expand( queryColumn, ORACLE_Connector.escape_column ) + ')';
				querystring += 'SELECT * FROM (';

				let not_exist_select = ' SELECT '+ Abstract_Connector.expand( queryColumn, ORACLE_Connector.escape_column ) + ' FROM ' + Abstract_Connector.escape_columns( query.table, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function ) + ' WHERE ';
				for( let i = 0; i < query.data.length; ++i )
				{
					querystring += ( i === 0 ? '' : ' UNION ' ) + ' ( ';

					for( let j = 0; j < queryColumn.length; ++j )
					{
						if( i === 0 )
						{
							not_exist_select += ( j === 0 ? '' : ' AND ' ) + ' ' + Abstract_Connector.escape_columns( 'ins_union.' + queryColumn[j], ORACLE_Connector.escape_column, ORACLE_Connector.transform_function ) + ' = ' + Abstract_Connector.escape_columns( query.table + '.' + queryColumn[j], ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
						}

						querystring += ( j === 0 ? ' SELECT ' : ',' ) + ORACLE_Connector.escape_value( ( ( query.data[i][queryColumn[j]] || query.data[i][queryColumn[j]] === 0 )  ? query.data[i][queryColumn[j]] : null ) ) + ' ' + Abstract_Connector.escape_columns( queryColumn[j], ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
					}
					querystring += ' FROM DUAL ) ';
				}

				querystring += '	) ' + Abstract_Connector.escape_columns( 'ins_union' , ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
				querystring += ' WHERE ';
				querystring += '	NOT EXISTS ( ' + not_exist_select + ' )';
			}
			else if( query.operation === 'insert' )  // todo pozriet na insert ak ma unique, ale default hodnota je prazdna
			{
				let clob_columns = [];
				if( query.tables && query.tables.hasOwnProperty( query.table ) )
				{
					for( let t_column in query.tables[ query.table ].columns ){ if( query.tables[ query.table ].columns[ t_column ].type === 'TEXT' ){ clob_columns.push(t_column); } }
				}

				querystring += 'INSERT INTO ' + Abstract_Connector.escape_columns( query.table, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function ) + ' (' + Abstract_Connector.expand( query.columns, ORACLE_Connector.escape_column ) + ') VALUES ';

				querystring += '(';
				for( let j = 0; j < query.columns.length; ++j )
				{
					querystring += ( j === 0 ? '' : ',' ) + '' + ( clob_columns.includes( query.columns[j] ) ? ' to_clob( :'+collumn_prefix + query.columns[j].replace(/(-)/i, '__Z__' )+')' : ' :'+collumn_prefix + query.columns[j].replace(/(-)/i, '__Z__' ) );
				}
				querystring += ')';
			}
			else if( query.operation === 'update' )
			{
				querystring += 'UPDATE ' + Abstract_Connector.escape_columns( query.table, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
			}
			else if( query.operation === 'delete' )
			{
				querystring += 'DELETE FROM ' + Abstract_Connector.escape_columns( query.table, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
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
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN (' + subquery + ') ' + query.join[i].table.alias + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column ), ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
					}
					else
					{
						querystring += Abstract_Connector.escape_columns( ( query.join[i].type == 'inner' ? ' INNER' : ' LEFT' ) + ' JOIN ' + query.join[i].table + ' ON ' + Abstract_Connector.expand_values( query.join[i].condition, query.join[i].data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column ), ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
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
							set += ( i ? ', ' : '' ) + ORACLE_Connector.escape_column(columns[i]) + ' = ' + ORACLE_Connector.escape_value(query.data[0][columns[i]]);
						}
					}

				}
				else if( query.data && Array.isArray(query.data) )
				{
					let columns = query.set.columns;
					let groups_indexes = query.set.indexes;

					let clob_columns = [];
					if( query.tables && query.tables.hasOwnProperty( query.table ) )
					{
						for( let t_column in query.tables[ query.table ].columns ){ if( query.tables[ query.table ].columns[ t_column ].type === 'TEXT' ){ clob_columns.push(t_column); } }
					}

					for( let i = 0; i < columns.length; ++i )
					{
						if( columns[i].substr(0,2) == '__' && columns[i].substr(-2) == '__' ){ continue; }

						set += ( i ? ', ' : '' ) + ORACLE_Connector.escape_column(columns[i]) + ' = CASE';

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
									set += ( k ? ' AND ' : '' ) + ORACLE_Connector.escape_column(indexes[k]) + ' = ' + ORACLE_Connector.escape_value(query.data[j][indexes[k]]);
								}

								if( clob_columns.includes( columns[i] ) )
								{
									set += ' THEN to_clob(' + ORACLE_Connector.escape_value(query.data[j][columns[i]])+')';
								}
								else { set += ' THEN ' + ORACLE_Connector.escape_value(query.data[j][columns[i]]); }
							}
						}

						set += ' ELSE ' + ORACLE_Connector.escape_column(columns[i]) + ' END';
					}
				}
				else
				{
					set = Abstract_Connector.escape_columns(query.set, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function);

					if(query.data !== null)
					{
						set = Abstract_Connector.expand_values(set, query.data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column);
					}
				}

				querystring += ' SET ' + set;
			}

			if( query.where )
			{
				let where = '';

				for( let i = 0; i < query.where.length; ++i )
				{
					let condition = Abstract_Connector.escape_columns( query.where[i].condition, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );

					if( typeof query.where[i].data !== 'undefined' )
					{
						condition = Abstract_Connector.expand_values( condition, query.where[i].data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column );
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
				let condition = Abstract_Connector.escape_columns( query.group_by.condition, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
				querystring += ' GROUP BY ' + ( query.group_by.data ? Abstract_Connector.expand_values( condition, query.group_by.data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column ) : condition );
			}

			if( query.having )
			{
				let condition = Abstract_Connector.escape_columns( query.having.condition, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
				querystring += ' HAVING ' + ( query.having.data ? Abstract_Connector.expand_values( condition, query.having.data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column ) : condition );
			}

			if( query.order )
			{
				let condition = Abstract_Connector.escape_columns( query.order.condition, ORACLE_Connector.escape_column, ORACLE_Connector.transform_function );
				querystring += ' ORDER BY ' + ( query.order.data ? Abstract_Connector.expand_values( condition, query.order.data, ORACLE_Connector.escape_value, ORACLE_Connector.escape_column ) : condition );
			}

			if( !query.offset && query.limit )
			{
				querystring = 'SELECT * FROM ('+querystring+') WHERE ROWNUM <= '+this.escape_value(query.limit);
			}
			else if( query.offset && query.limit )
			{
				querystring = 'SELECT * FROM ( SELECT a.*, ROWNUM '+this.escape_column('__RNUM__')+' FROM ( '+querystring+') a  WHERE ROWNUM <= '+this.escape_value( query.limit + query.offset ) + ' ) WHERE '+this.escape_column('__RNUM__')+'  > '+this.escape_value( query.limit );
			}
			else if( query.offset )
			{
				querystring = 'SELECT * FROM ( SELECT a.*, ROWNUM '+this.escape_column('__RNUM__')+' FROM ( '+querystring+') a ) WHERE '+this.escape_column('__RNUM__')+'  > '+this.escape_value( query.offset );
			}

			if( auto_increment )
			{
				//querystring = 'BEGIN EXECUTE IMMEDIATE '+this.escape_value(querystring)+';SELECT '+this.escape_column( query.table+'_seq' )+'.CURRVAL INTO :ret FROM DUAL; END';
			//	querystring += '; SET IDENTITY_INSERT ' + Abstract_Connector.escape_columns( query.table , ORACLE_Connector.escape_column ) + ' OFF;';
			}

			if( query.get_id && query.get_id.length )
			{
				let return_columns = [], into_columns = [];
				for( let i = 0; i < query.get_id.length; i++ )
				{
					return_columns.push( this.escape_column( query.get_id[i] ) );
					into_columns.push( ':'+query.get_id[i].replace(/(-)/i, '__Z__' ) );
				}

				querystring += ' RETURNING '+return_columns.join(', ')+' INTO '+ into_columns.join(', ');
			}

			querystring = querystring.replace(/!=\s+NULL/gi, 'IS NOT NULL' );
			querystring = querystring.replace(/>=\s+NULL/gi, '>= '+this.escape_value( ' ' ) );
			querystring = querystring.replace(/<=\s+NULL/gi, '<= '+this.escape_value( 'ZZZZZZZZZZZZZZZZZZZZZ' ) );
			querystring = querystring.replace(/=\s+NULL/gi, 'IS NULL' );  //TODO < > najmensi mozny string
			// SELE SLE treba refactornut

			return querystring;
		};

		this.execute = function( query, callback, indexes = null, auto_increment = null )
		{
			let operation = query.operation, insertData = null, insertOptions = null;
			if( typeof callback === 'undefined' ){ callback = null; }
			if( typeof query == 'object' )
			{
				if( operation === 'insert' )
				{
					insertData = [];

					let columns_sort = [];
					for( let i = 0; i < query.columns.length; i++ )
					{
						columns_sort.push( { column: query.columns[i], type: query.tables[ query.table ].columns[ query.columns[i] ].type } );
					}

					query.columns = [];
					columns_sort.sort( (a,b) => a.type === 'TEXT' ? 1 : -1 );

					for( let i = 0; i < columns_sort.length; i++ )
					{
						query.columns.push( columns_sort[i].column );
					}

					for( let i = 0; i < query.data.length; i++ )
					{
						let push_to_insertData = {};
						for( let column_name in query.data[i] )
						{
							let i_value = this.escape_value( query.data[i][ column_name ] );

							if( ( i_value && !isNaN(i_value)  ) || i_value === 0 ){ i_value = i_value.toString() }
							else if( i_value && i_value.substring( 0,1 ) === '\'' ){ i_value = i_value.substring(1, i_value.length -1 ); }
							push_to_insertData[ collumn_prefix + column_name.replace(/(-)/i, '__Z__' ) ] = i_value;
						}

						insertData.push( push_to_insertData );
					}

					insertOptions = {
						outFormat: oracle_sql.OBJECT,
						autoCommit: true,
						bindDefs: {}
					};

					if( query.get_id && query.get_id.length )
					{
						for( let i = 0; i < query.get_id.length; i++ )
						{
							let type = query.tables[ query.table ].columns[ query.get_id[i] ].type.split(/[\s,:]+/)[0];
							type = ( convertDataType.hasOwnProperty( type ) ? convertDataType[type] : type );

							let bind = { dir: oracle_sql.BIND_OUT, type: oracle_sql.STRING };
							bind.maxSize = 32767;

							insertOptions.bindDefs[ query.get_id[i].replace(/(-)/i, '__Z__' ) ] = bind;
						}
					}

					if( query.hasOwnProperty('tables') && query.tables.hasOwnProperty( query.table ) && query.tables[ query.table ].columns )
					{
						for( let j = 0; j < query.columns.length; ++j )
						{
							if( query.tables[ query.table ].columns.hasOwnProperty( query.columns[j] ) )
							{
								let coll = query.columns[j].replace(/(-)/i, '__Z__' );

								let type = query.tables[ query.table ].columns[ query.columns[j] ].type.split(/[\s,:]+/)[0];
								type = ( convertDataType.hasOwnProperty( type ) ? convertDataType[type] : type ) ;

								let bind = { dir: oracle_sql.BIND_IN, type: oracle_sql.STRING };
								bind.maxSize = 32767;

								insertOptions.bindDefs[ collumn_prefix + coll ] = bind;
							}
						}
					}
				}
				query = this.build( query, auto_increment );


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

				if( oracle_connections )
				{
					await oracle_connections.getConnection( async (err, conn) =>
					{
						if( !err )
						{
							if( operation === 'insert' )
							{
								await conn.executeMany( query, insertData, insertOptions , BIND_IF_FLOW( async (err, rows) =>
								{
									conn.release( function(err){});

									const elapsed_time = process.hrtime(start);
									result.sql_time = elapsed_time[0] * 1000 + elapsed_time[1] / 1000000;

									let elapsed = process.hrtime(start), remaining_ms = timeout_ms - elapsed[0] * 1000 - Math.ceil( elapsed[1] / 1e6 );

									if( err )
									{
										result.ok = false;
										result.error = err;
										err.code = result.error.errorNum;
										result.connector_error = new SQLError( err ).get();
									}
									else
									{
										if( rows.rows && rows.rows.length )
										{
											let column_type = {};
											if( rows.metaData && rows.metaData.length )
											{
												rows.metaData.forEach( md => { column_type[ md.name ] = md.dbType; })
											}

											for( let i = 0; i < rows.rows.length; ++i )
											{
												if( rows.rows[i].hasOwnProperty( '__RNUM__' ) ){ delete rows.rows[i]['__RNUM__']; }

												for( let c_name in rows.rows[i] )
												{
													if( column_type.hasOwnProperty( c_name ) && rows.rows[i][c_name] && column_type[ c_name ] === 2 && !isNaN(rows.rows[i][ c_name ]) && ( rows.rows[i][ c_name ].length < MIN_UINT.length || ( rows.rows[i][ c_name ].length == MIN_UINT.length && rows.rows[i][ c_name ] <= MIN_UINT ) ) )
													{
														rows.rows[i][ c_name ] = parseFloat( rows.rows[i][ c_name ] );
													}
													else if( column_type.hasOwnProperty( c_name ) && !rows.rows[i][c_name] && column_type[ c_name ] !== 2 )
													{
														rows.rows[i][ c_name ] = '';
													}
												}

												result.rows.push( rows.rows[i] );
											}

											result.row = result.rows[0];
										}

										result.sql_time +=  sql_time;
										result.affected_rows = rows.rowsAffected || result.rows.length;

										if( rows.rowsAffected && rows.outBinds && rows.outBinds.length )
										{
											rows.outBinds.forEach( iid =>
											{
												if( iid )
												{
													if( Object.keys( iid ).length === 1 )
													{
														let value = Object.values( iid )[0][0];
														if( !isNaN(value) && value && ( ( !isNaN(value) && typeof value === 'string' && value.indexOf( '.' ) !== -1 ) || value.length < MIN_UINT.length || ( value.length == MIN_UINT.length && value <= MIN_UINT ) ) )
														{
															let trimmed = value.trim();
															if( trimmed  ){ value = parseFloat( value ); }
														}
														result.inserted_ids.push(value);
														result.changed_ids.push(value);
													}
													else
													{
														let push_primary = {};

														for( let cid in iid )
														{
															let value = iid[cid][0];
															if( !isNaN(value) && value && ( ( !isNaN(value) && typeof value === 'string' && value.indexOf( '.' ) !== -1 ) || value.length < MIN_UINT.length || ( value.length == MIN_UINT.length && value <= MIN_UINT ) ) )
															{
																let trimmed = value.trim();
																if( trimmed  ){ value = parseFloat( value ); }
															}
															push_primary[cid] = value;
														}

														result.inserted_ids.push(push_primary);
														result.changed_ids.push(push_primary);
													}
												}
											});

											result.inserted_id = result.changed_id = result.inserted_ids[0];
											result.changed_rows = rows.changedRows || result.changed_ids.length;
										}
									}

									emit( 'query', result );
									if( callback ) { callback( result ); }
									else { resolve( result ); }
								}));
							}
							else
							{
								await conn.execute( query, {}, { outFormat: oracle_sql.OBJECT, autoCommit: true, extendedMetaData : true } , BIND_IF_FLOW( async (err, rows) =>
								{
									conn.release( function(err){});

									const elapsed_time = process.hrtime(start);
									result.sql_time = elapsed_time[0] * 1000 + elapsed_time[1] / 1000000;

									let elapsed = process.hrtime(start), remaining_ms = timeout_ms - elapsed[0] * 1000 - Math.ceil( elapsed[1] / 1e6 );

									if( err )
									{
										result.ok = false;
										result.error = err;
										err.code = result.error.errorNum;
										result.connector_error = new SQLError( err ).get();
									}
									else
									{
										if( rows.rows && rows.rows.length )
										{
											let column_type = {};
											if( rows.metaData && rows.metaData.length )
											{
												rows.metaData.forEach( md => { column_type[ md.name ] = md.dbType; })
											}

											for( let i = 0; i < rows.rows.length; ++i )
											{
												if( rows.rows[i].hasOwnProperty( '__RNUM__' ) ){ delete rows.rows[i]['__RNUM__']; }

												for( let c_name in rows.rows[i] )
												{
													if( column_type.hasOwnProperty( c_name ) && rows.rows[i][c_name] && column_type[ c_name ] === 2 && !isNaN(rows.rows[i][ c_name ]) && ( rows.rows[i][ c_name ].length < MIN_UINT.length || ( rows.rows[i][ c_name ].length == MIN_UINT.length && rows.rows[i][ c_name ] <= MIN_UINT ) ) )
													{
														rows.rows[i][ c_name ] = parseFloat( rows.rows[i][ c_name ] );
													}
													else if( column_type.hasOwnProperty( c_name ) && !rows.rows[i][c_name] && column_type[ c_name ] !== 2 )
													{
														rows.rows[i][ c_name ] = '';
													}
												}

												result.rows.push( rows.rows[i] );
											}

											result.row = result.rows[0];
										}

										result.sql_time +=  sql_time;
										result.affected_rows = rows.rowsAffected || result.rows.length;

										if( rows.affectedRows && rows.insertId )
										{
											if( auto_increment )
											{
												for( let i = 0; i < rows.affectedRows; ++i )
												{
													result.inserted_ids.push( rows.insertId - rows.affectedRows + 1 + i );
													result.changed_ids.push( rows.insertId + i );
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
											result.changed_rows = rows.changedRows || result.changed_ids.length;
										}
									}

									emit( 'query', result );
									if( callback ) { callback( result ); }
									else { resolve( result ); }
								}));
							}
						}
						else
						{
							result = { ok : false, error: 'neconecti' };
							if( callback ) { callback( result ); }
							else { resolve( result ); }
						}
					});
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
			return 'SELECT Distinct TABLE_NAME FROM information_schema.TABLES';  //TODO
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
					//	columns[ name ].update = 'CURRENT_TIMESTAMP';
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
			'WHERE t.is_oracle_shipped = 0 AND ind.object_id = object_id(' + this.escape_value( table ) + ') ' +
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
			let columns = [], indexes = [], after_table = [];
			let querystring = 'CREATE TABLE ' + this.escape_column( name );

			for( let column in table.columns )
			{
				if( table.columns.hasOwnProperty( column ) )
				{
					let generated = this.create_column( table.columns[ column ], column, name );

					let columnData =  ' ' + this.escape_column( column ) + ' ' + generated.column;

					if( generated.after ){ after_table = after_table.concat( generated.after ); }

					columns.push( columnData );
				}
			}

			for( let type in table.indexes )
			{
				if( table.indexes.hasOwnProperty(type) && table.indexes[type] && table.indexes[type].length )
				{
					let keys = ( typeof table.indexes[type] === 'string' ? [ table.indexes[type] ] : table.indexes[type] );

					keys.forEach( key => {
						if( type === 'index' )
						{
							let index = this.create_index( key, type, table.columns, name );
							if( index ){ after_table.push( index ); }
						}
						else
						{
							let index = this.add_index( key, type, table.columns, name );
							if( index ){ indexes.push( index ); }
						}
					});
				}
			}

			querystring += ' (' + columns.concat( indexes ).join(',') + ' )';



			let trans = 'BEGIN EXECUTE IMMEDIATE '+this.escape_value( querystring )+'; ';

			if( after_table.length ){ after_table.forEach( query_t => { trans += ' EXECUTE IMMEDIATE ' + this.escape_value( query_t )+'; '; }); }
			return trans +'  END;';
		};

		this.drop_table = function( table )
		{
			return 'BEGIN '+
				'EXECUTE IMMEDIATE ' + this.escape_value( 'BEGIN EXECUTE IMMEDIATE ' + this.escape_value( 'DROP TABLE '+this.escape_column(table)+' CASCADE CONSTRAINTS' ) + '; EXCEPTION WHEN OTHERS THEN NULL; END;' )+';'+
				'EXECUTE IMMEDIATE ' + this.escape_value( 'BEGIN EXECUTE IMMEDIATE ' + this.escape_value( 'DROP SEQUENCE '+this.escape_column( this.generate_sequence_name( table, null, 'seq' ))) + '; EXCEPTION WHEN OTHERS THEN NULL; END;' )+';'+
				' END;';
		};

		this.create_column = function( data, name, table )
		{
			let column = '', after = [];

			if( data )
			{
				if( data.type )
				{
					let type = data.type.split(/[\s,:]+/)[0];

					column += ' ' + ( convertDataType.hasOwnProperty( type ) ? convertDataType[type] : type ) ;

					let match = data.type.match(/:([0-9]+)/);

					if( type === 'DECIMAL' )
					{
						column += '('+data.type.match(/:([0-9,]+)/)[1]+')';
					}
					else if( match )
					{
						if( ['VARCHAR'].indexOf( type ) > -1 && match[1] > 4000 )
						{
							column = ' ' + convertDataType['TEXT'];
						}
						else { column += '(' + match[1] + ')'; }
					}
					else if( type == 'INT' )
					{
						column += '(11)';
					}
					else if( type == 'BIGINT' )
					{
						column += '(20)';
					}
					else if( ['SET', 'ENUM'].indexOf( type ) > -1 )
					{
						column += '(' + data.type.split(':')[1].trim().length  + ')';
					}


					if( data.multibyte )
					{
					//	column += ' CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci';
					}



					if( typeof data.default !== 'undefined' )
					{
						if( ['TIMESTAMP'].indexOf(type) > -1 && data.default === '0000-00-00 00:00:00')
						{
							data.default = '01-JAN-00 01.00.00.00000';
						}

						column += ' DEFAULT ' + ( ['CURRENT_TIMESTAMP', 'NULL'].indexOf(data.default) === -1 ? this.escape_value( data.default.toString() ) : data.default );
					}
					else if( ['SET', 'VARCHAR', 'TEXT'].includes( type ) )
					{
						column += ' NOT NULL DISABLE';
					}
					else
					{
						column += ( data.null ? ' NOT NULL DISABLE' : ' NOT NULL ENABLE'  );
					}

					if( data.increment )
					{
						let sequenceName = this.generate_sequence_name( table, null, 'seq' );
						after.push( 'CREATE SEQUENCE ' + this.escape_column( sequenceName ) + ' START WITH 1' );
						after.push( 'CREATE OR REPLACE TRIGGER ' + this.generate_index_name( name, table, null, 'inc_bir' ) + ' BEFORE INSERT ON ' + this.escape_column( table ) + ' FOR EACH ROW BEGIN IF :new.'+this.escape_column(name)+' is null THEN SELECT ' + this.escape_column( sequenceName ) + '.NEXTVAL INTO :new.'+this.escape_column(name)+' FROM dual; END IF; END;' );
						//after.push( 'CREATE OR REPLACE TRIGGER ' + this.generate_index_name( name, table, null, 'set_max' ) + ' AFTER INSERT ON ' + this.escape_column( table ) + ' BEGIN "alterSequenceLast"( 10000 ); END;' );
					}

					if( false && data.update )
					{
						after.push( '\n CREATE OR REPLACE TRIGGER ' + this.generate_index_name( name, table, null, 'time_bir' ) +
							'\n BEFORE INSERT OR UPDATE ON ' + this.escape_column( table ) + '' +
							'\n FOR EACH ROW' +
							'\n BEGIN' +
							'\n   :new.' + this.escape_column( name ) + ' := SYSTIMESTAMP;' +
							'\n END;\n/' );
					}

					if( ['SET'].indexOf( type ) > -1 )
					{
						column += ' CONSTRAINT ' + this.generate_index_name( name, table, null, 'cons' ) + ' CHECK ( ' + this.escape_column(name) + ' IN (' + Abstract_Connector.expand( data.type.split(':')[1].trim().split(/\s*,\s*/).concat( '' ), this.escape_value ) + ') )';
					}

					if( ['ENUM'].indexOf( type ) > -1 )
					{
						column += ' CONSTRAINT ' + this.generate_index_name( name, table, null, 'cons' ) + ' CHECK ( ' + this.escape_column(name) + ' IN (' + Abstract_Connector.expand( data.type.split(':')[1].trim().split(/\s*,\s*/), this.escape_value ) + ') )';
					}


					if( data.unsigned )
					{
						column += ' CONSTRAINT ' + this.generate_index_name( name, table, null, 'uns' ) + ' check ( ' + this.escape_column(name) + ' > 0 )';
					}
				}
			}

			return { column, after };
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

		this.add_index = function( index, type, columns, table, alter )
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

		this.create_index = function( index, type, columns, table, alter )
		{
			if( !alter ){ alter = false; }

			let cols = index.split(/\s*,\s*/);
			let sql = ( alter ? 'CREATE ' : ' CREATE ' ) + ( ['primary', 'unique'].indexOf( type ) === -1 ? 'INDEX ' : ' CONSTRAINT ' ) + this.generate_index_name( cols.join(','), table, ( type === 'primary' ? 'PK' : ( type === 'unique' ? 'UC' : 'I' )  ), null ) + ' ' + ( type === 'primary' ? 'PRIMARY KEY ' : ( type === 'unique' ? ( alter ? 'UNIQUE ' : 'UNIQUE ' ) : ( type === 'index' ? ( alter ? 'INDEX ' : ' ' ) : '' ) ) ) + ' ON '+this.escape_column( table )+'(';

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

		function shortName( word, max_length, parse_by = '_' )
		{
			let short = '', parse_length = Math.floor( max_length / word.split( '_' ).length );

			word.split( parse_by ).forEach( word_part => { if( word_part ) { short += capitalize( word_part.substr( 0, Math.min( word_part.length, parse_length ))); }});

			return short;
		}

		this.generate_index_name = function( columns, table, prefix, type )
		{
			let constraintName = [], max_index_length = 30, index_length = 0;

			if( typeof columns === 'string' ){ columns = columns.split( ',' ); }

			if( prefix ){ constraintName.push( prefix ); index_length += prefix.length; }
			if( type ){ index_length += type.length; }

			let index_string = table + '_' + columns.join('_');

			if( ( index_length + index_string.length ) > max_index_length )
			{
				let indexName = shortName( index_string, ( max_index_length - index_length ) );

				if( indexName ){ constraintName.push( indexName )};
			}
			else{ constraintName.push( index_string ); }

			if( type ) { constraintName.push( type.toUpperCase() ); }

			if( constraintName.join('').length > 30 )
			{
				//
			}

			return constraintName.join('');
		};

		this.generate_sequence_name = function( table, prefix, type ) //todo options max_sequence_length, split by
		{
			let constraintName = [], max_sequence_length = 26, sequence_length = 0;

			if( prefix ){ constraintName.push( prefix ); sequence_length += prefix.length; }
			if( table )
			{
				sequence_length += table.length;

				if( sequence_length > max_sequence_length )
				{
					let splitedTable = shortName( table, max_sequence_length );

					if( splitedTable ){ constraintName.push( splitedTable ); }
				}
				else{ constraintName.push( table ); }
			}

			if( type ){ constraintName.push( '_'+type ); }

			if( constraintName.join('').length > 30 )
			{
				//
			}

			return constraintName.join('');
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
			return oracle_connected;
		};

		connect();

	})();
};
