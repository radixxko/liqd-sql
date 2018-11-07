'use strict';

const TimedPromise = require('liqd-timed-promise');
const SQLError = require( './errors.js');
const MAX_TIMEOUT_MS = 180000;

if( !Array.prototype.values )
{
	Object.defineProperty( Array.prototype, 'values',
	{
		value: Array.prototype[Symbol.iterator],
		configurable: false,
		writable: false
	});
}

module.exports = class Connector
{
	constructor( connect )
	{
		var db_connector = this.db_connector = connect.db_connector;
		this.emit = connect.emit;
		this.tables = connect.tables;
		this.ping_interval = null;
		this.offline_queries = [];

		this.ping = function()
		{
			db_connector.ping_query();
		}

		this.resetPingInterval = function( interval )
		{
			if( this.ping_interval ){clearInterval( this.ping_interval ) };
			this.ping_interval = setInterval( this.ping, interval );
		}

		this.resetPingInterval( 1000 );

		connect.on( 'status', (status) =>
		{
			if( status === 'connected' )
			{
				this.resetPingInterval( 10000 );

				let offline_query, now = (new Date()).getTime();

				while( offline_query = this.offline_queries.shift() )
				{
					if( offline_query.deadline > now + 100 )
					{
						//this.execute_query( offline_query.query, offline_query.callback, offline_query.deadline - now );
						this.execute_query( offline_query.query, offline_query.callback, offline_query.indexes, offline_query.auto_increment );
					}
				}
			}
			else if( status === 'disconnected' )
			{

				this.resetPingInterval( 1000 );
			}
		});
	}


	_subquery( table, alias = undefined )
	{
		let Query = require('./query');
		return new Query({ table, alias, connector: this, tables: this.tables });
	}

	execute_query( query, callback, indexes, auto_increment = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let start = process.hrtime(); let timeout_ms = Math.min( remaining_ms, MAX_TIMEOUT_MS );

			this.db_connector.execute( query, result =>
			{
				let elapsed = process.hrtime(start), remaining_ms = timeout_ms - elapsed[0] * 1000 - Math.ceil( elapsed[1] / 1e6 );

				if( result.connector_error && result.connector_error.reconnect && remaining_ms > 100 )
				{
					if( this.db_connector.connected() )
					{
						this.execute_query( query, callback, indexes )
							.timeout( remaining_ms )
							.catch( e => reject( e ));
					}
					else
					{
						this.offline_queries.push({ deadline: (new Date()).getTime() + remaining_ms, query, callback, indexes, auto_increment });
					}
				}
				else
				{
					resolve( callback( result ) );
					this.resetPingInterval( 10000 );
				}
			}, indexes, auto_increment )
			.timeout( timeout_ms )
			.catch( e => reject( e ));
		});
	}

	showColumns( query )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( query.columns && query.columns.columns.indexOf( '*' ) !== -1 )
			{
				let all_tables = false, columns_by_tables = {};
				let tables_columns = this.db_connector.get_tables_columns( query.columns.columns );

				if( query.columns.columns === '*' || tables_columns.indexOf('*') !== -1 ){ all_tables = true; }

				let [ table, alias ] = query.table.split(/\s+/i);
				let word = ( alias ? alias : table );

				if( all_tables || ( tables_columns && tables_columns.indexOf( word ) !== -1 ) )
				{
					if( !this.tables || ( this.tables && !this.tables.hasOwnProperty( table ) ) )
					{
						await this.describe_table( table, 'columns' );
					}

					if( this.tables && this.tables.hasOwnProperty( table ) )
					{
						for( let column in this.tables[ table ].columns )
						{
							if( this.tables[ table ].columns.hasOwnProperty( column ) )
							{

								if( !columns_by_tables.hasOwnProperty( word ) ){ columns_by_tables[ word ] = []; }
								columns_by_tables[ word ].push( word + '.' + column );
							}
						}
					}
				}

				if( query.join && query.join.length )
				{
					for( let i = 0; i < query.join.length; i++ )
					{
						if( query.join[i].table )
						{
							if( typeof query.join[i].table === 'string' )
							{
								let [ table, alias ] = query.join[i].table.split(/\s+/i);
								let word = ( alias ? alias : table );

								if( all_tables || tables_columns.indexOf( word ) !== -1 )
								{
									if( !this.tables || ( this.tables && !this.tables.hasOwnProperty( table ) ) )
									{
										await this.describe_table( table, 'columns' );
									}

									if( this.tables && this.tables.hasOwnProperty( table ) )
									{
										for( let column in this.tables[ table ].columns )
										{
											if( this.tables[ table ].columns.hasOwnProperty( column ) )
											{
												if( !columns_by_tables.hasOwnProperty( word ) ){ columns_by_tables[ word ] = []; }
												columns_by_tables[ word ].push( word + '.' + column );
											}
										}
									}
								}
							}
							else if( typeof query.join[i].table === 'object' && ( all_tables || tables_columns.indexOf( query.join[i].table.alias ) !== -1 ) )
							{
								if( query.join[i].table.columns.columns.indexOf( '*' ) !== -1 )
								{
									await this.showColumns( query.join[i].table );
								}

								if( !columns_by_tables.hasOwnProperty( query.join[i].table.alias ) ){ columns_by_tables[ query.join[i].table.alias ] = []; }

								let previous_columns = query.join[i].table.columns.columns.split( ',' );
								for( let k = 0; k < previous_columns.length; k++ )
								{
									let words = previous_columns[k].split(/\s+/i);
									words = words[ words.length-1 ].split('.');
									columns_by_tables[ query.join[i].table.alias ].push( query.join[i].table.alias + '.' + words[ words.length-1 ] );
								}
							}
						}
					}
				}

				if( all_tables )
				{
					let new_query_columns = [];
					for( let column in columns_by_tables ){ if( columns_by_tables.hasOwnProperty( column ) ){ new_query_columns = new_query_columns.concat( columns_by_tables[ column ] ); } }

					let used_columns = [];
					query.columns.columns.split(',').forEach( ( column ) => { if( column.trim() === '*' ){ used_columns.push( new_query_columns.join(', ') ); }else{ used_columns.push( column ); } });
					query.columns.columns = used_columns.join( ', ' );
				}
				else
				{
					tables_columns.forEach( ( column ) => { if( columns_by_tables.hasOwnProperty( column ) ){ query.columns.columns = query.columns.columns.replace( column+'.*', columns_by_tables[ column ].join( ', ' ) ); } });
				}
			}

			resolve('end');
		});
	}

	query_for_update( query, indexes, columns )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let query_for_changes = JSON.parse( JSON.stringify( query )), before_update = new Map();;
			query_for_changes.columns = { columns: columns.join(', '), data: null };
			query_for_changes.operation = 'select';
			query_for_changes.set = null;

			let select_query = this.db_connector.build( query_for_changes );

			this.execute_query( select_query, async ( result ) =>
			{
				if( result.rows.length && result.rows.length < 10000 )
				{
					for( var i = 0; i < result.rows.length; ++i )
					{
						let key_values = [];
						indexes.forEach( column => key_values.push( result.rows[i][ column ] ));
						before_update.set( key_values.join( '-' ), result.rows[i]);
					}
				}

				resolve({ query: select_query, before_update : before_update });

			}, indexes )
				.timeout( remaining_ms )
				.catch( e => reject( e ));
		});
	}

	query_after_update( query, indexes, before_update, result  )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			this.execute_query( query, async ( result_after ) =>
			{
				if( result_after.rows.length )
				{
					for( var i = 0; i < result_after.rows.length; ++i )
					{
						let key_values = [], primary = {};
						indexes.forEach( column =>
						{
							key_values.push( result_after.rows[i][ column ] );
							primary[ column ] = result_after.rows[i][ column ];
						});

						let previous = before_update.get( key_values.join( '-' ));

						if( previous )
						{
							for( let column in result_after.rows[i] )
							{
								if( previous.hasOwnProperty( column ) && result_after.rows[i].hasOwnProperty( column ) &&  result_after.rows[i][ column ] !== previous[ column ] )
								{
									result.changed_ids.push( ( Object.keys( primary ).length === 1 ? Object.values( primary )[0] : primary ));
									break;
								}
							}
						}
					}
				}

				if( result.changed_ids.length )
				{
					result.changed_id = result.changed_ids[0];
					result.changed_rows = result.changed_ids.length;
				}

				resolve( result );
			}, indexes )
				.timeout( remaining_ms )
				.catch( e => reject( e ));
		});
	}

	_create_datum( row, existing_row = null, changed_columns = null ) // todo skontrolovat ci to ide pre vsetky indexi co vrati existing rows
	{
		var datum = {}, changed = !Boolean(existing_row);

		for( var column in row )
		{
			var value, existing_value, escaped = false;

			if( '&?!'.indexOf(column[0]) === -1 )
			{
				value = row[column];
				existing_value = ( existing_row ? existing_row[column] : undefined );
			}
			else
			{
				var type = (column.match(/^[&?!]*/)[0] || ''),
						row_value = row[column],
						column = column.substr(type.length),
						existing_value = ( existing_row ? existing_row[column] : undefined );

				if( type.includes('!') )
				{
					if( row_value )
					{
						 value = row_value;

						if( type.includes('&') ){ escaped = true; }
					}
					else{ value = ( existing_value ? existing_value : ( typeof row_value == 'number' ? 0 : '' ) ); }
				}
				else if( type.includes('?') )
				{
					if( typeof existing_value !== 'undefined' )
					{
						if( typeof row_value === 'object' )
						{
							if( typeof row_value[existing_value] !== 'undefined' )
							{
								value = row_value[existing_value];
							}
							else if( typeof row_value['&'+existing_value] !== 'undefined' )
							{
								value = row_value['&'+existing_value]; escaped = true;
							}
							else
							{
								value = existing_value;
							}
						}
						else
						{
							value = existing_value;
						}
					}
					else
					{
						if( typeof row_value === 'object' )
						{
							if( typeof row_value['&_default'] != 'undefined' )
							{
								value = row_value['&_default']; escaped = true;
							}
							else{ value = row_value['_default'] || ''; }
						}
						else
						{
							value = row_value;

							if( type.includes('&') ){ escaped = true; }
						}
					}
				}
				else if( type.includes('&') )
				{
					value = row_value; escaped = true;
				}
			}

			changed = changed || ( value != existing_value );

			if( !existing_row || value != existing_value || ( existing_row.__indexes__ &&	existing_row.__indexes__.indexOf(column) !== -1 ) )
			{
				datum[column] = ( escaped ? '&__escaped__:' + value : value );
			}

			if( existing_row && value != existing_value && changed_columns && changed_columns.indexOf(column) === -1 )
			{
				changed_columns.push(column);
			}
		}

		if( changed && existing_row && existing_row.__indexes__ )
		{
			datum.__indexes__ = existing_row.__indexes__;

			if( typeof existing_row.__primary__ != 'undefined' )
			{
				datum.__primary__ = existing_row.__primary__;
			}
		}

		return ( changed ? datum : null );
	}

	_get_main_indexes( query )
	{
		if( this.tables )
		{
			if( this.tables[query.table] && this.tables[query.table].indexes )
			{
				if( this.tables[query.table].indexes.primary )
				{
					if( typeof this.tables[query.table].indexes.primary == 'string' )
					{
						return this.tables[query.table].indexes.primary.split(/\s*,\s*/);
					}
				}

				if( this.tables[query.table].indexes.unique )
				{
					if( typeof this.tables[query.table].indexes.unique == 'string' )
					{
						return this.tables[query.table].indexes.unique.split(/\s*,\s*/);
					}
					else if( this.tables[query.table].indexes.unique.length )
					{
						return this.tables[query.table].indexes.unique[0].split(/\s*,\s*/);
					}
				}
			}
		}

		return null;
	}

	_get_all_indexes( query )
	{
		var indexes = [];

		if( this.tables )
		{
			if( this.tables[query.table] && this.tables[query.table].indexes )
			{
				if( this.tables[query.table].indexes.primary )
				{
					if( typeof this.tables[query.table].indexes.primary == 'string' )
					{
						indexes.push( this.tables[query.table].indexes.primary.split(/\s*,\s*/) );
					}
				}

				if( this.tables[query.table].indexes.unique )
				{
					if( typeof this.tables[query.table].indexes.unique == 'string' )
					{
						indexes.push( this.tables[query.table].indexes.unique.split(/\s*,\s*/) );
					}
					else for( var i = 0; i < this.tables[query.table].indexes.unique.length; ++i )
					{
						indexes.push( this.tables[query.table].indexes.unique[i].split(/\s*,\s*/) );
					}
				}
			}

			return indexes;
		}
	}

	_get_used_indexes( data, query )
	{
		var indexes = [], indexes_groups = [], indexes_order = [], indexes_groups_iterators = [], indexes_unique_groups = [];

		if( this.tables[query.table] && this.tables[query.table].indexes )
		{
			if( this.tables[query.table].indexes.primary )
			{
				if( typeof this.tables[query.table].indexes.primary == 'string' )
				{
					indexes.push( this.tables[query.table].indexes.primary.split(/\s*,\s*/) );
				}
			}

			if( this.tables[query.table].indexes.unique )
			{
				if( typeof this.tables[query.table].indexes.unique == 'string' )
				{
					indexes.push( this.tables[query.table].indexes.unique.split(/\s*,\s*/) );
				}
				else for( var i = 0; i < this.tables[query.table].indexes.unique.length; ++i )
				{
					indexes.push( this.tables[query.table].indexes.unique[i].split(/\s*,\s*/) );
				}
			}
		}

		for( let i = 0; i < indexes.length; ++i )
		{
			indexes_order.push(i);
			indexes_groups.push([]);
			indexes_groups_iterators.push(0);
			indexes_unique_groups.push([]);
		}

		for( let i = 0; i < data.length; ++i )
		{
			const datum = data[i];

			for( let j = 0; j < indexes.length; ++j )
			{
				let is_index = true, index = indexes[j];

				for( let k = 0; k < index.length; ++k )
				{
					if( typeof datum[index[k]] === 'undefined' ){ is_index = false; break; }
				}

				if( is_index ){ indexes_groups[j].push(i); }
			}
		}

		indexes_order.sort( ( a, b ) => { return indexes_groups[b].length > indexes_groups[a].length } );

		var result = {};

		if( indexes_groups.length == 0 )
		{
			return result;
		}

		for( let i = 0; i < data.length; ++i )
		{
			for( let j = 0; j < indexes_order.length; ++j )
			{
				const index = indexes_order[j];

				while( indexes_groups[index][indexes_groups_iterators[index]] < i ){ ++indexes_groups_iterators[index]; }

				if( indexes_groups[index][indexes_groups_iterators[index]] == i )
				{
					indexes_unique_groups[index].push(i);
				}
			}
		}

		for( let i = 0; i < indexes_unique_groups.length; ++i )
		{
			if( indexes_unique_groups[i].length )
			{
				result[ indexes[i].join(',') ] = indexes_unique_groups[i];
			}
		}

		return result;
	}

	_get_all_columns( data, columns = [] )
	{
		columns = [];

		for( var i = 0; i < data.length; ++i )
		{
			for( var column in data[i] )
			{
				column = column.replace(/^[&!?]+/,'');

				if( columns.indexOf( column ) === -1 )
				{
					columns.push( column );
				}
			}
		}

		return columns;
	}

	_get_all_existing_columns( data, columns = [], table )
	{
		columns = [];
		let table_column = this.tables[table].columns;

		for( var i = 0; i < data.length; ++i )
		{
			for( var column in data[i] )
			{
				column = column.replace(/^[&!?]+/,'');

				if( columns.indexOf( column ) === -1 && table_column.hasOwnProperty( column ) )
				{
					columns.push( column );
				}
			}
		}

		if( this.tables[table].indexes.primary )
		{
			let primary = this.tables[table].indexes.primary.split(/\s*,\s*/);

			if( primary.length == 1 && columns.indexOf( primary[0] ) === -1 )
			{
				columns.push( primary[0] );
			}
		}

		return columns;
	}

	_get_all_table_columns( table, columns = [] )
	{
		if( this.tables[table].columns ){ columns = Object.keys( this.tables[table].columns ); }

		return columns;
	}

	_generate_where_for_indexes( group_indexes, data, data_filter = null )
	{
		var data_index_to_i = new Map();
		var indexed_data = new Map();
		var conditions = [], condition_data = { _glue_: '_' };

		if( !data_filter )
		{
			data_filter = [];

			for( var i = 0; i < data.length; ++i )
			{
				data_filter.push(i);
			}
		}

		for( var indexes of group_indexes )
		{
			var data_indexes = new Map();
			var condition = '';

			for( var i = 0; i < indexes.length; ++i ){ data_indexes.set( indexes[i], [] ); }

			if( indexes.length > 1 )
			{
				data_indexes.set( '_concat', [] );
			}

			var prefix = indexes.join('_') + '__';

			for( var i of data_filter )
			{
				if( indexed_data.has(i) ){ continue; }
				if( data[i].__indexes__ && data[i].__indexes__.join(',') != indexes.join(',') ){ continue; }

				let empty = false;
				for( let j = 0; j < indexes.length; ++j )
				{
					if( !data[i] || !data[i][indexes[j]] ) { empty = true; break; }
				}
				if(empty){ continue; }

				var index = '';

				for( var j = 0; j < indexes.length; ++j )
				{
					var value = data[i][indexes[j]];
					index += ( index ? '_' : '' ) + value;
					var data_index = data_indexes.get( indexes[j] );

					if( value && data_index.indexOf(value) === -1 ){ data_index.push(value); }
				}

				data_index_to_i.set(prefix + index, i);
				indexed_data.set(i, true);

				if( indexes.length > 1 )
				{
					data_indexes.get( '_concat').push(index);
				}
			}

			for( var i = 0; i < indexes.length; ++i )
			{
				let data_for_indexes = data_indexes.get(indexes[i]);

				if( data_for_indexes.length )
				{
					condition += ( condition ? ' AND ' : '' ) + indexes[i] + ' IN (:' + ( prefix + indexes[i] ) + ')';
					condition_data[prefix + indexes[i]] = data_for_indexes;
				}
			}

			if( indexes.length > 1 )
			{
				let data_for_concat = data_indexes.get('_concat');

				if( data_for_concat.length )
				{
					condition += ( condition ? ' AND ' : '' ) + ' CONCAT(' + indexes.join(',:_glue_,') + ') IN (:' + prefix + 'CONCATENATED)';
					condition_data[prefix + 'CONCATENATED'] = data_for_concat;
				}
			}

			if( condition )
			{
				conditions.push(condition);
			}
		}

		return { condition: ( conditions.length ? '( ' + conditions.join(' ) OR ( ') +	' )' : '' ), data: condition_data, index: data_index_to_i	};
	}

	_get_existing_rows( data, query )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !this.tables || !this.tables.hasOwnProperty( query.table ) ){ await this.describe_table( query.table ); }

			if( this.tables && this.tables.hasOwnProperty( query.table ))
			{
				var indexes = this._get_used_indexes( data, query );
				var rows_index = {}, time = 0;

				for( let index in indexes )
				{
					var existing = await this._get_existing_rows_for_index( data,	index.split(','), indexes[index], rows_index, query.table ).timeout( remaining_ms ).catch( (e, remaining_ms ) => e );
					time +=	existing.time;

					if( existing.error ){ resolve({	ok: false, error: existing.error }); }
				}

				resolve({ ok: true, rows: rows_index, sql_time: time });
			}
			else{ resolve({	ok: false, error: new SQLError({ code: 'UNDEFINED_TABLE' }).get() }); }
		});
	}

	_get_existing_rows_for_index( data, indexes, data_indexes, rows_index, table )
	{
		var columns = this._get_all_existing_columns( data, indexes, table );
		var where = this._generate_where_for_indexes( [ indexes ], data, data_indexes );

		return this._subquery( table ).where( where.condition, where.data ).get_all( columns ).then( ( rows ) =>
		{
			if( rows.ok )
			{
				let primary = ( this.tables[table].indexes.primary ? this.tables[table].indexes.primary.split(/\s*,\s*/) : null );
				primary = ( primary.length == 1 ? primary[0] : null );

				for( var i = 0; i < rows.rows.length; ++i )
				{
					rows.rows[i].__indexes__ = indexes;

					if( primary && typeof rows.rows[i][primary] != 'undefined' )
					{
						rows.rows[i].__primary__ = rows.rows[i][primary];
					}

					var id = '';

					for( let j = 0; j < indexes.length; ++j )
					{
						id += ( id ? '_' : '' ) + rows.rows[i][indexes[j]];
					}

					var prefix = indexes.join('_') + '__', index = where.index.get(prefix + id);

					if( index !== null )
					{
						rows_index[index] = rows.rows[i];
					}
				}

				return { error: null, time: rows.sql_time };
			}
			else{ return { error: rows.error, time: 0 }; }
		});
	}

	describe_table( table, option = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let described_table = {}, error = null;

			if( !option || option === 'columns' )
			{
				let columns = await this.describe_columns( table );

				if( columns.ok ){ described_table[ 'columns' ] = columns.columns; }
				else { error = columns.error; }
			}

			if( !option || option === 'indexes' )
			{
				let indexes = await this.describe_indexes( table );

				if( indexes.ok ){ described_table[ 'indexes' ] = indexes.indexes; }
				else { error = indexes.error; }
			}

			if( error ) { resolve({	ok: false, error: error, connector_error: new SQLError( error ).get() }); }
			else
			{
				if( !this.tables ){ this.tables = {}; }

				if( !this.tables.hasOwnProperty(table))
				{
					this.tables[table] = {
						columns: ( described_table.columns ? described_table.columns : null ),
						indexes: ( described_table.indexes ? described_table.indexes : null )
					};
				}
				else
				{
					this.tables[table].columns = ( described_table.columns ? described_table.columns : this.tables[table].columns || null );
					this.tables[table].indexes = ( described_table.indexes ? described_table.indexes : this.tables[table].indexes || null );
				}

				resolve({ ok: true, table: this.tables[table]  });
			}
		});
	}

	describe_columns( table, option = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			this.execute_query( this.db_connector.getColumnsQuery( table ), async ( result ) =>
			{
				let columns = null;

				if( result.ok && result.rows.length )
				{
					columns = await this.db_connector.describe_columns( result.rows );

					resolve({ ok: result.ok, error: result.error, columns });
				}
				else
				{
					result.ok = false;
					result.error = result.error || new SQLError({ code: 'UNDEFINED_TABLE' }).get();

					resolve({ ok: result.ok, error: result.error, columns });
				}
			});
		});
	}

	describe_indexes( table, option = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			this.execute_query( this.db_connector.getIndexesQuery( table ), async ( result ) =>
			{
				let indexes = null;

				if( result.ok && result.rows.length )
				{
					indexes = await this.db_connector.describe_indexes( result.rows );
				}

				resolve({ ok: result.ok, error: result.error, indexes });
			});
		});
	}

	create_database( database, tables, options )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( database && tables )
			{
				let queries = [];

				queries.push( this.db_connector.create_database_query( database ) );

				for( let table in tables )
				{
					if( tables.hasOwnProperty( table ) )
					{
						if( options.drop_table ){ queries.push( this.drop_table_query( table )); }

						queries.push( this.create_table_query( tables[table], table ));

						if( options.defaultRows && options.defaultRows.hasOwnProperty( table ) )
						{
							queries.push( options.defaultRows[ table ] );
						}
					}
				}

				resolve( ( options.result_type === 'array' ? queries : ( options.format ? queries.join('\n') : queries.join(' '))));
			}
			else { resolve( { ok: false, error: 'empty_data' }); }
		});
	}

	modify_database( modified_tables, options )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			modified_tables = JSON.parse( JSON.stringify( modified_tables ) );

			let alterTables = [];
			let ctns = 0;

			let database = await this.database();

			if( database.ok )
			{
				let tables = database.tables;

				if( options.drop_table )
				{
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

							if( drop_table ){ alterTables.push( this.drop_table_query( oldTableName )); }
						}
					}
				}

				for( let table in modified_tables )
				{
					if( modified_tables.hasOwnProperty( table ) )
					{
						let originTableName = ( modified_tables[table]['renamed'] ? modified_tables[table]['renamed'] : table ), alterTable = '', afterUpdate = '';

						if( !tables.hasOwnProperty( originTableName ) && tables.hasOwnProperty( table ) ){ originTableName = table; }

						if( tables.hasOwnProperty( originTableName ) )
						{
							let alterTableHeader = '  ALTER TABLE ' + this.db_connector.escape_column( originTableName ) + ( originTableName !== table ? ' RENAME ' + this.db_connector.escape_column( table ) : '' ) ;
							let afterUpdateHeader = '  ALTER TABLE ' + this.db_connector.escape_column( table );
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
							if( alters.length > 0 ) { alterTables.push( alters ); }
						}
						else
						{
							alterTables.push( [ this.create_table_query( modified_tables[table], table ) ] );
						}

						if( options.defaultRows && options.defaultRows.hasOwnProperty( table ) )
						{
							alterTables.push( options.defaultRows[ table ] );
						}
					}
				}

				resolve( ( alterTables.length ? alterTables.join('; ') : 'Nothing to modify' ));
			}
			else{ resolve( { ok: false, database: database }); }
		});
	}

	database()
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			this.execute_query( this.db_connector.getTablesQuery(), async ( tables ) =>
			{
				let databaseTables = {};
				if( tables.rows && tables.rows.length > 0 )
				{
					for( let i = 0; i < tables.rows.length; i++ )
					{
						for( let info in tables.rows[i] )
						{
							if( tables.rows[i].hasOwnProperty( info ) )
							{
								let described = await this.describe_table( tables.rows[i][ info ]);

								if( described.ok ) { databaseTables[ tables.rows[i][ info ] ] = described.table; }
							}
						}
					}

					resolve({ ok: true, tables: databaseTables });
				}
				else { resolve({ ok: false, error: 'empty_tables' }); }
			});
		});
	}

	create_table_query( table, alias = null, options = [] )
	{
		return this.db_connector.create_table( table, alias, options );
	}

	drop_table_query( table, options = [] )
	{
		return this.db_connector.drop_table( table, options );
	}

	compare_columns( current_table, modified_table, table, originTable )
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

					if( drop_column ){ alterColumns.push( this.db_connector.drop_column_query( originColumn ) ); }
				}
				else
				{
					if( current_table.columns[ originColumn ].type.split(/[\s,:]+/)[0] === 'TIMESTAMP' && current_table.columns[ originColumn ].default && current_table.columns[ originColumn ].default !== 'CURRENT_TIMESTAMP' )
					{
						updateBeforeAlter.push( 'UPDATE ' + this.db_connector.escape_column( originTable ) + ' SET ' + this.db_connector.escape_column( originColumn ) + ' = \'2000-01-01 01:01:01\' WHERE ' + this.db_connector.escape_column( originColumn ) + ' < \'1000-01-01\' ' );
					}
				}
			}
		}

		for( let column in modified_table.columns )
		{
			if( modified_table.columns.hasOwnProperty( column ) )
			{
				let originColumnName = ( modified_table.columns[ column ].renamed ? modified_table.columns[ column ].renamed : column );
				let modiefiedColumnData = this.db_connector.create_column( modified_table.columns[ column ] );

				if( current_table.columns.hasOwnProperty( originColumnName ) )
				{
					let currentColumnData = this.db_connector.create_column( current_table.columns[ originColumnName ] );

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
									setValues += 'IF( ' + this.db_connector.escape_column( column ) + ' = ' + this.db_connector.escape_value( changeValue ) + ', ' + this.db_connector.escape_value( modified_table.columns[column].change[ changeValue ] ) + ', ';
									ended += ')';
								}
							}

							setValues += ' ' + this.db_connector.escape_column( column ) + ended;
							temporaryColumns[ column ] =  'tmp_' + column;
							modifiedColumn = this.modify_column( column, modiefiedColumnData, previousColumn, setValues );
						}
						else if( currentType === 'TIMESTAMP' && modiefiedType === 'BIGINT' )
						{
							setValues += 'UNIX_TIMESTAMP( ' + this.db_connector.escape_column( column ) + ' )';
							temporaryColumns[ column ] =  'tmp_' + column;
							modifiedColumn = this.modify_column( column, modiefiedColumnData, previousColumn, setValues );
						}
						else if( modified_table.columns[ column ].data && currentType !== modiefiedType )
						{
							setValues += Abstract_Connector.escape_columns( modified_table.columns[ column ], this.db_connector.escape_column ) ;
							temporaryColumns[ column ] =  'tmp_' + column;
							modifiedColumn = this.modify_column( column, modiefiedColumnData, previousColumn, setValues );
						}
						else
						{
							alterColumns.push( '   CHANGE ' + this.db_connector.escape_column( originColumnName ) + ' ' + this.db_connector.escape_column( column ) + ' ' + this.db_connector.create_column( modified_table.columns[column] ) + ( previousColumn !== '' ? ' AFTER ' + this.db_connector.escape_column( previousColumn ) : '' ) );
						}

						if( modifiedColumn.alterColumns ) { alterColumns = alterColumns.concat( modifiedColumn.alterColumns ); }
						if( modifiedColumn.updateValues ) { updateValues = updateValues.concat( modifiedColumn.updateValues ); }
						if( modifiedColumn.afterUpdateColumns ) { afterUpdateColumns = afterUpdateColumns.concat( modifiedColumn.afterUpdateColumns ); }
					}
				}
				else { alterColumns.push( '   ADD ' + this.db_connector.escape_column( column ) + modiefiedColumnData + ( previousColumn !== '' ? ' AFTER ' + this.db_connector.escape_column( previousColumn ) : '' ) ); }

				previousColumn = column;
			}
		}

		if( updateValues.length > 0 )
		{
			update = 'UPDATE ' + this.db_connector.escape_column( table ) + ' SET ' + updateValues.join( ',' );
		}

		return { updateBeforeAlter: updateBeforeAlter, alterColumns: alterColumns, update: update, afterUpdateColumns: afterUpdateColumns, temporaryColumns: temporaryColumns};
	}

	compare_indexes( newIndex, oldIndex, columns, tmp_columns, table )
	{
		let alter = [], indexes = [ 'primary', 'unique', 'index' ];

		indexes.forEach( type => {
			let primary = ( type === 'primary' ), newIndexesName = [];

			if( newIndex[ type ] && !Array.isArray(newIndex[ type ]) ){ newIndex[ type ] = [newIndex[ type ]]; }

			if( newIndex && newIndex[ type ] && newIndex[ type ].length > 0 )
			{
				newIndex[ type ].forEach( index => newIndexesName.push( ( primary ? 'PRIMARY' : this.db_connector.generate_index_name( index, table, ( type === 'primary' ? 'PK' : ( type === 'unique' ? 'UC' : '' )), null ) ) ) );
			}

			if( newIndex && typeof newIndex[ type ] === 'string' ){ newIndex[ type ] = [ newIndex[ type ] ]; }

			if( oldIndex && oldIndex[ type ] )
			{
				if( Object.keys( oldIndex[ type ] ).length > 0 )
				{
					if( newIndex && newIndex[ type ] && newIndex[ type ].length > 0 )
					{
						if( !primary )
						{
							if( Array.isArray( oldIndex[ type ] ) )
							{
								oldIndex[ type ].forEach( old_index => {
									let exist = false;

									for( let i = 0; i < newIndex[ type ].length; i++ )
									{
										if( old_index && !primary && newIndex[ type ][i] && old_index === newIndex[ type ][i] ){ exist = true; break; }
									}

									if( !exist )
									{
										alter.push( this.db_connector.drop_index( this.db_connector.generate_index_name( old_index, table, ( type === 'primary' ? 'PK' : ( type === 'unique' ? 'UC' : '' )), null ), type ) );
									}
								});
							}
							else
							{
								for( let indexName in oldIndex[ type ] )
								{
									if( oldIndex[ type ].hasOwnProperty( indexName ) )
									{
										let exist = false;

										for( let i = 0; i < newIndex[ type ].length; i++ )
										{
											if( oldIndex[ type ].hasOwnProperty(indexName) && !primary && newIndex[ type ][i] && oldIndex[ type ][ indexName ] === newIndex[ type ][i] ){ exist = true; break; }
										}

										if( !exist ){ alter.push( this.db_connector.drop_index( indexName, type ) ); }
									}
								}
							}
						}
					}
					else{ alter.push( this.db_connector.drop_index( Object.keys( oldIndex[ type ] ), type ) ); }
				}
			}

			if( newIndex && newIndex[ type ] && newIndex[ type ].length > 0 )
			{
				newIndex[ type ].forEach( new_index =>
				{
					if( new_index )
					{
						let generatedIndexName = ( primary ? 'PRIMARY' : this.db_connector.generate_index_name( new_index, table, ( type === 'primary' ? 'PK' : ( type === 'unique' ? 'UC' : '' )), null ));
						let new_index_columns = [], split_index = new_index.split(',');

						new_index.split(',').forEach( index => {
							new_index_columns.push( index );
							if( tmp_columns.hasOwnProperty( index ) ){ new_index_columns.push( tmp_columns[ index ] ); }
						});

						new_index = new_index_columns.join(',');

						let changed = true;

						if( Array.isArray( oldIndex[ type ] ) )
						{
							oldIndex[ type ].forEach( old_index => { if( new_index === old_index ){ changed = false; } });
						}
						else if( typeof oldIndex[ type ] === 'string')
						{
							if( new_index === oldIndex[ type ] ){ changed = false; }
						}
						else
						{
							for( let indexName in oldIndex[ type ] )
							{
								if( oldIndex[ type ].hasOwnProperty( indexName ) )
								{
									if(newIndex[type][u] === oldIndex[type][indexName]){ changed = false; }
								}
							}
						}

						if( changed )
						{
							if( oldIndex[ type ].hasOwnProperty( generatedIndexName ) )
							{
								if( oldIndex[ type ][ generatedIndexName] !== new_index )
								{
									alter.push( this.db_connector.drop_index( generatedIndexName, type ) );
									alter.push( this.db_connector.create_index( new_index, type, columns, table, true ) );
								}
							}
							else{ alter.push( this.db_connector.create_index( new_index, type, columns, table, true ) ); }
						}
					}
				});
			}
		});

		return ( alter.length > 0 ? ' ' + alter.join( ', ' ) : '' );
	}
}
