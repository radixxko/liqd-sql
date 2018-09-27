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

function expandWhere( filter, escape_value )
{
	if( !filter ){ return null; }
	else if( typeof filter === 'string' ){ return filter; }
	else
	{
		var conditions = [];

		for( let column in filter )
		{
			let type = (column.match(/^[&?!]*/)[0] || ''),
				table_column = column.substr(type.length);

			if( Array.isArray(filter[column]) )
			{
				var conditionValues = [];

				for( var i = 0; i < filter[column].length; ++i )
				{
					conditionValues.push(( type.includes('&') ? filter[column][i] : escape_value(filter[column][i])));
				}

				if( conditionValues.length > 0 ){ conditions.push(table_column + ( type.includes('!') ? ' NOT ' : ' ' ) + 'IN (' + conditionValues.join(', ') + ')'); }
			}
			else
			{
				if( filter[column] == null )
				{
					conditions.push( table_column + ' IS ' + ( type.includes('!') ? 'NOT ' : ' ' ) + 'NULL');
				}
				else
				{
					conditions.push( table_column + ( type.includes('!') ? ' !' : ' ' ) + '= ' + escape_value(filter[column]));
				}
			}
		}

		return conditions.join(' AND ');
	}
}

class SQLQuery
{
	constructor( query, callback, remaining_ms )
	{
		this.start = process.hrtime();
		this.query = query;
		this.callback = callback;
		this.timeout_ms = remaining_ms;
	}

	elapsed()
	{
		let elapsed = process.hrtime( this.start );

		return elapsed[0]*1000 + elapsed[1]/1e6 > this.timeout_ms;
	}
}

module.exports = class Query
{
	constructor( query )
	{
		this.query = { table: query.table, options: [] };
		this.connector = query.connector;
		this.tables = query.tables;

		if( query.alias ){ this.query.alias = query.alias; }
	}

	_subquery( table, alias = undefined )
	{
		return new Query({ table, alias, connector: this.connector, tables: this.tables });
	}

	join( table, condition, data = null )
	{
		if( table && condition )
		{
			if(!this.query.join)
			{
				this.query.join = [];
			}

			this.query.join.push({table: ( table.query	? table.query : table ), condition: condition, data: data});
		}

		return this;
	}

	inner_join( table, condition, data = null )
	{
		if( table && condition )
		{
			if(!this.query.join)
			{
				this.query.join = [];
			}

			this.query.join.push({table: ( table.query	? table.query : table ), condition: condition, data: data, type: 'inner'});
		}
		return this;
	}

	union( union )
	{
		if( union )
		{
			if( !this.query.union )
			{
				this.query.union = [];

				if( this.query.table )
				{
					this.query.union.push( ( this.query.table.query ? this.query.table.query : this.query.table ) );
				}
			}

			if( Array.isArray( union ) )
			{
				union.forEach( ( query ) => { this.query.union.push( ( query.query	? query.query : query ) ); });
			}
			else { this.query.union.push( ( union.query	? union.query : union ) ); }
		}

		return this;
	}

	where( condition, data = null )
	{
		if( condition )
		{
			if( !this.query.where )
			{
				this.query.where = [];
			}

			if( Array.isArray( condition ) || condition instanceof Map || condition instanceof Set )
			{
				const entries = condition; condition = '';

				for( var entry of entries.values() )
				{
					condition += ( condition ? ' OR ' : '' ) + '(';

					for( var column in entry )
					{
						condition += this.connector.db_connector.escape_column( column ) + ' = ' + this.connector.db_connector.escape_value( entry[column] ) + ' AND ';
					}

					condition = condition.substr(0, condition.length - 5) + ')';
				}
			}
			else if( typeof condition == 'object' )
			{
				condition = expandWhere(condition, this.connector.db_connector.escape_value );
			}

			if( condition )
			{
				this.query.where.push({ condition: condition, data: data });
			}
		}

		return this;
	}

	order_by( condition, data = null )
	{
		if( condition )
		{
			this.query.order = { condition: condition, data: data };
		}

		return this;
	}

	order( condition, data = null )
	{
		if( condition )
		{
			this.query.order = { condition: condition, data: data };
		}

		return this;
	}

	limit( limit )
	{
		if( limit )
		{
			this.query.limit = limit;
		}

		return this;
	}

	offset( offset )
	{
		if( offset )
		{
			this.query.offset = offset;
		}

		return this;
	}

	group_by( condition, data = null )
	{
		if( condition )
		{
				this.query.group_by = { condition: condition, data: data };
		}

		return this;
	}

	having( condition, data = null )
	{
		if( condition )
		{
				this.query.having = { condition: condition, data: data };
		}

		return this;
	}

	escape( value )
	{
		return this.connector.db_connector.escape_value( value );
	}

	map( index )
	{
		this.query.map = { index: index };

		return this;
	}

	columns( columns = '*', data = null )
	{
		this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };

		return this;
	}

	execute()
	{
		let set_start_time = process.hrtime();
		if( this.query.table )
		{
			return new TimedPromise( async( resolve, reject, remaining_ms ) =>
			{
				this.connector.execute_query( this.query.table, ( result ) =>
				{
					let set_elapsed_time = process.hrtime(set_start_time);
					result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

					resolve( result );
				})
				.timeout( remaining_ms )
				.catch( e => reject( e ));
			});
		}
		else{ return { ok : false, error: { code: 'EMPTY_QUERY' } }; }
	}

	select_row_query( columns = '*', data = null, alias = null )
	{
		return this.get_query( columns, data, alias );
	}

	select_row( columns = '*', data = null, alias = null )
	{
		return this.get( columns, data, alias );
	}

	select_query( columns = '*', data = null, alias = null )
	{
		return this.get_all_query( columns, data, alias );
	}

	select( columns = '*', data = null, alias = null )
	{
		return this.get_all( columns, data, alias );
	}

	get_union( alias = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( this.query.union && this.query.union.length )
			{
				this.query.operation = 'union';

				if( alias )
				{
					resolve( '( ' + this.connector.db_connector.build( this.query ) + ' ) ' + alias + ' ' );
				}
				else
				{
					resolve( this.connector.db_connector.build( this.query ));
				}
			}
			else{ resolve( { ok : false, error: { code: 'UNDEFINED_TABLE' }}); }
		});
	}

	get_query( columns = '*', data = null, alias = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( this.query.table || ( this.query.union && this.query.union.length ) )
			{
				this.query.operation = 'select';
				this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };
				this.query.limit = 1;

				if( this.query.group_by &&  typeof this.query.table === 'string' && !this.query.table.includes( '(' ))
				{
					await this.connector.showColumns( this.query );
				}

				if( alias )
				{
					resolve( '( ' + this.connector.db_connector.build( this.query ) + ' ) ' + alias + ' ' );
				}
				else
				{
					resolve( this.connector.db_connector.build( this.query ));
				}
			}
			else{ resolve({ ok : false, error: { code: 'UNDEFINED_TABLE' } }); }
		});
	}

	get( columns = '*', data = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let set_start_time = process.hrtime();
			if( this.query.table || ( this.query.union && this.query.union.length ) )
			{
				this.query.operation = 'select';
				this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };
				this.query.limit = 1;

				if( this.query.group_by &&  typeof this.query.table === 'string' && !this.query.table.includes( '(' ))
				{
					await this.connector.showColumns( this.query );
				}

				let elapsed_time = process.hrtime(set_start_time); remaining_ms = remaining_ms - Math.ceil( elapsed_time[0]*1e3 + elapsed_time[1]/1e6 );

				if( remaining_ms > 0 )
				{

					this.connector.execute_query( this.query, ( result ) =>
					{
						let set_elapsed_time = process.hrtime(set_start_time);
						result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

						resolve( result );
					})
					.timeout( remaining_ms )
					.catch( e => reject( e ));
				}
			}
			else{ resolve( { ok : false, error: { code: 'UNDEFINED_TABLE' } }); }
		});
	}

	get_all_query( columns = '*', data = null, alias = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( this.query.table || ( this.query.union && this.query.union.length ) )
			{
				this.query.operation = 'select';
				this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };

				if( this.query.group_by &&  typeof this.query.table === 'string' && !this.query.table.includes( '(' ))
				{
					await this.connector.showColumns( this.query );
				}

				if( alias )
				{
					resolve( '( ' + this.connector.db_connector.build( this.query ) + ' ) ' + alias + ' ' );
				}
				else
				{
					resolve( this.connector.db_connector.build( this.query ));
				}
			}
			else{ resolve( { ok : false, error: { code: 'UNDEFINED_TABLE' } }); }
		});
	}

	get_all( columns = '*', data = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let set_start_time = process.hrtime();
			if( this.query.table || ( this.query.union && this.query.union.length ) )
			{
				this.query.operation = 'select';
				this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data } ;

				if( this.query.group_by &&  typeof this.query.table === 'string' && !this.query.table.includes( '(' ))
				{
					await this.connector.showColumns( this.query );
				}

				let elapsed_time = process.hrtime(set_start_time); remaining_ms = remaining_ms - Math.ceil( elapsed_time[0]*1e3 + elapsed_time[1]/1e6 );

				if( remaining_ms > 0 )
				{
					this.connector.execute_query( this.query, ( result ) =>
					{
						if( this.query.map )
						{
							result.map = new Map();

							for( var i = 0; i < result.rows.length; ++i )
							{
								 result.map.set(result.rows[i][this.query.map.index], result.rows[i]);
							}
						}

						let set_elapsed_time = process.hrtime(set_start_time);
						result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

						resolve( result );
					})
					.timeout( remaining_ms )
					.catch( e => { reject( e )});
				}
			}
			else{ resolve( { ok : false, error: { code: 'UNDEFINED_TABLE' }}); }
		});
	}

	delete()
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let set_start_time = process.hrtime();
			if( this.query.table )
			{
				this.query.operation = 'delete';

				this.connector.execute_query( this.query, ( result ) =>
				{
					let set_elapsed_time = process.hrtime(set_start_time);
					result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;
					resolve( result );
				})
					.timeout( remaining_ms )
					.catch( e => reject( e ));;
			}
			else{ resolve( { ok : false, error: { code: 'UNDEFINED_TABLE' }}); }
		});
	}

	set( data )
	{
		let set_start_time = process.hrtime();
		if( this.query.table )
		{
			if( data && ( Array.isArray( data ) ? data.length : typeof data === 'object' ) )
			{
				if( !Array.isArray( data ) ){ data = [ data ]; }

				return this.connector._get_existing_rows( data, this.query ).then( async( existing ) =>
				{
					if( !this.tables || !this.tables[this.query.table] )
					{
						let described = await this.connector.describe_table( this.query.table );
						if( described.ok )
						{
							if( !this.tables ){ this.tables = {}; }
							this.tables[this.query.table] = described.table;
						}
					}

					if( existing.ok )
					{
						var result = { ok: true, error: null, affected_rows: 0, changed_rows:	0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], sql_time: existing.sql_time };
						let insert_data = [], update_data = [], update_columns = [], changed_ids = [];

						for( var i = 0; i < data.length; ++i )
						{
							var datum = Object.keys(data[i]).filter(column =>	this.tables[this.query.table].columns.hasOwnProperty(column.replace(/^[&!?]+/,''))).reduce((obj, column) => { obj[column] = data[i][column]; return obj; }, {});
									datum = this.connector._create_datum( datum, existing.rows[i], update_columns );

							if( typeof existing.rows[i] === 'undefined' )
							{
								insert_data.push(datum);
							}
							else if( datum )
							{
								update_data.push(datum);
							}
							else if( existing.rows[i] ) { result.affected_rows++ }
						}

						if( insert_data.length )
						{
							var inserted = await this._subquery( this.query.table ).insert( insert_data );

							result.sql_time += inserted.sql_time;

							if( inserted.ok )
							{
								result.affected_rows	+= inserted.affected_rows;
								result.changed_rows		+= inserted.changed_rows;
								result.inserted_id		= result.inserted_id || inserted.inserted_id;
								result.inserted_ids		= result.inserted_ids.concat( inserted.inserted_ids );;
								result.changed_id		= result.changed_id || inserted.changed_id;
								result.changed_ids		= result.changed_ids.concat( inserted.changed_ids );
								result.row				= result.row || inserted.row;
								result.rows				= result.rows.concat( inserted.rows );
							}
							else{ return { ok: false, error: inserted.error } }
						}

						if( update_data.length && update_columns.length )
						{
							var update_set = update_columns;
							var updated_query = this._subquery( this.query.table );

							if( this.query.where ){ updated_query.query.where = JSON.parse(JSON.stringify( this.query.where )); }

							let updated = await updated_query.update( update_set, update_data );

							result.sql_time += updated.sql_time;

							if( updated.ok )
							{
								result.affected_rows	+= updated.affected_rows;
								result.changed_rows		+= updated.changed_rows;
								result.inserted_id		= result.inserted_id || updated.inserted_id;
								result.inserted_ids		= result.inserted_ids.concat( updated.inserted_ids );;
								result.changed_id		= result.changed_id || updated.changed_id;
								result.changed_ids		= result.changed_ids.concat( updated.changed_ids );
								result.row				= result.row || updated.row;
								result.rows				= result.rows.concat( updated.rows );

								/*
								for( let changed_id of changed_ids )
								{
									if( result.changed_ids.indexOf(changed_id) === -1 )
									{
										result.changed_ids.push(changed_id);
									}
								}
								*/
							}
							else { return { ok: false, error: updated.error }; }
						}

						let set_elapsed_time = process.hrtime(set_start_time);
						result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

						return result;
					}
					else { return { ok: false, error: existing.error }; }
				});
			}
			else
			{
				let set_elapsed_time = process.hrtime(set_start_time);
				return new TimedPromise(( resolve ) => { resolve( { ok: true, error: null, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], time: set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000 } ) });
			}
		}
		else
		{
			return new TimedPromise(( resolve ) => { resolve( { ok: false, error: { code: 'UNDEFINED_TABLE' }, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] } ) });
		}
	}

	insert( data, ignore = false )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let set_start_time = process.hrtime();

			if( this.query.table )
			{
				if( data && ( Array.isArray( data ) ? data.length : typeof data == 'object' ) )
				{
					if( !this.tables || !this.tables.hasOwnProperty(this.query.table) )
					{
						let described = await this.connector.describe_table( this.query.table );
						if( described.ok )
						{
							if( !this.tables ){ this.tables = {}; }
							this.tables[this.query.table] = described.table;
						}
					}

					if( !Array.isArray( data ) ){ data = [ data ]; }

					this.query.operation = 'insert';
					this.query.data = data;
					this.query.columns = this.connector._get_all_columns( data );

					if( ignore ){ this.query.options.push('ignore'); }
					let autoIncrementColumn = null, is_auto_increment;

					if( this.tables && this.tables[this.query.table].columns )
					{
						for( let columnName in this.tables[this.query.table].columns )
						{
							if( this.tables[this.query.table].columns.hasOwnProperty( columnName ) && this.tables[this.query.table].columns[columnName][ 'increment'] ){ autoIncrementColumn = columnName; break; }
						}

						if( autoIncrementColumn )
						{
							for( let i = 0; i < data.length; i++ )
							{
								if( data[i].hasOwnProperty( autoIncrementColumn ) ) { is_auto_increment = true; break;  }
							}
						}
					}

					this.connector.execute_query( this.query, async ( result ) =>
					{
						let set_elapsed_time = process.hrtime(set_start_time);
						result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

						let indexes = this.connector._get_main_indexes( this.query );

						if( indexes )
						{
							var inserted_ids = [], changed_ids = [];

							for( var i = 0; i < this.query.data.length; ++i )
							{
								var index_value = null, successful = true;

								indexes.forEach( index =>
								{
									if( typeof this.query.data[i][index] != 'undefined' )
									{
										if( indexes.length > 1 )
										{
											if( !index_value ){ index_value = {}; }

											index_value[ index ] = this.query.data[i][index];
										}
										else { index_value = this.query.data[i][index]; }
									}
									else{ successful = false; }
								});

								if( successful )
								{
									inserted_ids.push(index_value);
									changed_ids.push(index_value);
								}
								else{ break; }
							}

							result.inserted_id		= result.inserted_id || inserted_ids[0];
							result.inserted_ids		= ( result.inserted_ids.length ? result.inserted_ids : inserted_ids );
							result.changed_id		= result.changed_id || inserted_ids[0];
							result.changed_ids		= ( result.changed_ids.length ? result.changed_ids : changed_ids );
						}

						resolve( result );
					}, null, is_auto_increment )
						.timeout( remaining_ms )
						.catch( e => reject( e ));
				}
				else
				{
					let set_elapsed_time = process.hrtime( set_start_time );
					resolve( { ok: true, error: null, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], time: set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000 });
				}
			}
			else
			{
				resolve( { ok: false, error: { code: 'UNDEFINED_TABLE' }, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] });
			}
		});
	}

	update( set, data = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let set_start_time = process.hrtime();

			if( this.query.table )
			{
				if( set )
				{
					let error = null;

					if( !Array.isArray( set ) && typeof set === 'object' && !data )
					{
						data = [ set ];
						set = Object.keys(set).join(',');
					}
					else if( Array.isArray( set ) && typeof set[0] === 'object' && !data )
					{
						data = set;
						set = Object.keys(set[0]).join(','); // TODO prejst cez vsetky polozky/stlpce a zagregovat
					}

					this.query.operation = 'update';
					this.query.set = set;
					this.query.data = data;

					if( !this.tables )
					{
						let described = await this.connector.describe_table( this.query.table );
						if( !described.ok ){ error = described; }
					}

					if( data && Array.isArray(data) && !error )
					{
						this.query.set = { indexes: this.connector._get_all_indexes( this.query ), columns: Array.isArray(this.query.set) ? this.query.set : this.query.set.split(/\s*,\s*/) };

						if( this.query.where )
						{
							this.query.update_with_where = true;
						}

						if( this.query.set.indexes )
						{
							let where = this.connector._generate_where_for_indexes( this.query.set.indexes, data );
							this.where( where.condition, where.data );
						}

						if( !this.query.set.indexes.length && !this.query.where ){ error = { error: { code: 'INVALID_ENTRY' }, connector_error: new SQLError( 'INVALID_ENTRY' ).get() }; }
					}

					if( !error )
					{
						let indexes = this.connector._get_main_indexes( this.query ), columns = this.connector._get_all_table_columns( this.query.table );
						let query_for_changes = await this.connector.query_for_update( this.query, indexes, columns );

						this.connector.execute_query( this.query, async ( result ) =>
						{
							if( query_for_changes && query_for_changes.query )
							{
								result = await this.connector.query_after_update( query_for_changes.query, indexes, query_for_changes.before_update, result );
							}

							let set_elapsed_time = process.hrtime(set_start_time);
							result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

							resolve( result );
						}, indexes )
						.timeout( remaining_ms )
						.catch( e => reject( e ));
					}
					else
					{
						resolve( { ok: false, error: error.error, connector_error: error.connector_error, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] });
					}
				}
				else
				{
					let set_elapsed_time = process.hrtime(set_start_time);
					resolve( { ok: true, error: null, connector_error: null, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], time: set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000 });
				}
			}
			else
			{
				resolve( { ok: false, error: { code: 'UNDEFINED_TABLE' }, connector_error: null, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] } );
			}
		});
	}

	create_database( database, tables, options = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !options )
			{
				options = { result_format: 'string' };
				//defaultRows, dropTables
			}

			tables = JSON.parse( JSON.stringify( tables ) );

			//if( defaultRowsPath ){ defaultRows = require(  )  } //TODO cez path

			if( database && tables )
			{
				resolve( { ok: true, create: await this.connector.create_database( database, tables, options )});
			}
			else { resolve({ ok: false, error: 'empty_data' }); }
		});
	}

	modify_database( tables, options = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !options )
			{
				options = { result_format: 'string' };
				//defaultRows, dropTables
			}

			tables = JSON.parse( JSON.stringify( tables ) );

			//if( defaultRowsPath ){ defaultRows = require(  )  } //TODO cez path

			if( tables )
			{
				resolve( { ok: true, create: await this.connector.modify_database( tables, options )});
			}
			else { resolve({ ok: false, error: 'empty_data' }); }
		});
	}

	create_table( execute, options = [] )
	{
		let set_start_time = process.hrtime();
		let query = this.connector.create_table_query( this.query.table, this.query.alias, options );

		if( execute )
		{
			return new TimedPromise( async( resolve, reject, remaining_ms ) =>
			{
				this.connector.execute_query( query, ( result ) =>
				{
					let set_elapsed_time = process.hrtime(set_start_time);
					result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

					resolve( result );
				})
				.timeout( remaining_ms )
				.catch( e => reject( e ));
			});
		}
		else { return query; }
	}

	drop_table( execute, options = [] )
	{
		let set_start_time = process.hrtime();
		let query = this.connector.drop_table_query( this.query.table, options );

		if( execute )
		{
			return new TimedPromise( async( resolve, reject, remaining_ms ) =>
			{
				this.connector.execute_query( query, ( result ) =>
				{
					let set_elapsed_time = process.hrtime(set_start_time);
					result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

					resolve( result );
				})
				.timeout( remaining_ms )
				.catch( e => reject( e ));
			});
		}
		else { return query; }
	}
}
