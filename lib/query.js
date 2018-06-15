'use strict';

const TimedPromise = require('liqd-timed-promise');
const SQLError = require( './errors.js');

if( !Array.prototype.values )
{
	Object.defineProperty( Array.prototype, 'values',
	{
		value: Array.prototype[Symbol.iterator],
		configurable: false,
		writable: false
	});
}

function expandWhere( filter )
{
	if( !filter ){ return null; }
	else if( typeof filter === 'string' ){ return filter; }
	else
	{
		var conditions = [];

		for( var column in filter )
		{
			if( Array.isArray(filter[column]) )
			{
				var conditionPositive = [];
				var conditionNegative = [];

				for( var i = 0; i < filter[column].length; ++i )
				{
					if( filter[column][i][0] === '!' )
					{
						conditionNegative.push(( column.substr(0, 1) == '&' ? filter[column][i].substr(1) : '"' + (filter[column][i].substr(1)) + '"' ));
					}
          else
          {
            conditionPositive.push(( column.substr(0, 1) == '&' ? filter[column][i] : '"' + (filter[column][i]) + '"' ));
          }
        }

        if( column.substr(0, 1) == '&' ){ column = column.substr(1); }
        if( conditionPositive.length > 0 ){ conditions.push(column + ' IN (' + conditionPositive.join(', ') + ')'); }
        if( conditionNegative.length > 0 ){ conditions.push(column + ' NOT IN (' + conditionNegative.join(', ') + ')'); }
      }
      else
      {
        if( filter[column] == null )
        {
          conditions.push(( column.substr(0, 1) == '&' ? column.substr(1) : column ) + ' IS NULL');
        }
        else if( filter[column][0] == '!' )
        {
          conditions.push(( column.substr(0, 1) == '&' ? column.substr(1) : column ) + ' != "' + (filter[column].substr(1)) + '"');
        }
        else
        {
          conditions.push(( column.substr(0, 1) == '&' ? column.substr(1) : column ) + ' = "' + (filter[column]) + '"');
        }
      }
    }

    return conditions.join(' AND ');
  }
}

module.exports = class Query
{
  constructor( query )
  {
    this.query = { table: query.table, options: [] };
		this.connector = query.connector;
		this.emit = query.emit;
    this.tables = query.tables;

    if( query.alias ){ this.query.alias = query.alias; }
  }

  _subquery( table, alias = undefined )
  {
    return new Query({ emit: this.emit, table, alias, connector: this.connector, tables: this.tables });
  }

  join( table, condition, data = null )
  {
    if( table && condition )
    {
      if(!this.query.join)
      {
          this.query.join = [];
      }

      this.query.join.push({table: ( table.query  ? table.query : table ), condition: condition, data: data});
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

      this.query.join.push({table: ( table.query  ? table.query : table ), condition: condition, data: data, type: 'inner'});
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
        union.forEach( ( query ) => { this.query.union.push( ( query.query  ? query.query : query ) ); });
      }
      else { this.query.union.push( ( union.query  ? union.query : union ) ); }
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
            condition += this.connector.escape_column( column ) + ' = ' + this.connector.escape_value( entry[column] ) + ' AND ';
          }

          condition = condition.substr(0, condition.length - 5) + ')';
        }
      }
      else if( typeof condition == 'object' )
      {
        condition = expandWhere(condition);
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
		return this.connector.escape_value( value );
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
        let result = await this.connector.select( this.query.table );

        if( !result.ok ){ result.error = new SQLError( result.error.code ).get(); }

        let set_elapsed_time = process.hrtime(set_start_time);
        result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

        resolve( result );
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

  get_all_query( columns = '*', data = null, alias = null )
  {
    return this.get_all_query( columns, data, alias );
  }

  select( columns = '*', data = null, alias = null )
  {
    return this.get_all( columns, data, alias );
  }

  async get_union( alias = null )
  {
    if( this.query.union && this.query.union.length )
    {
      this.query.operation = 'union';

      if( alias )
      {
        return '( ' + this.connector.build( this.query ) + ' ) ' + alias + ' ';
      }
      else
      {
        return this.connector.build( this.query );
      }
    }
    else{ return { ok : false, error: { code: 'UNDEFINED_TABLE' } }; }
  }

  async get_query( columns = '*', data = null, alias = null )
  {
    if( this.query.table || ( this.query.union && this.query.union.length ) )
    {
      this.query.operation = 'select';
      this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };
      this.query.limit = 1;

      if( this.query.group_by )
      {
        await this.showColumns( this.query );
      }

      if( alias )
      {
        return '( ' + this.connector.build( this.query ) + ' ) ' + alias + ' ';
      }
      else
      {
        return this.connector.build( this.query );
      }
    }
    else{ return { ok : false, error: { code: 'UNDEFINED_TABLE' } }; }
  }

  async get( columns = '*', data = null )
	{
    let set_start_time = process.hrtime();
    if( this.query.table || ( this.query.union && this.query.union.length ) )
    {
    	this.query.operation = 'select';
    	this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };
    	this.query.limit = 1;

      if( this.query.group_by )
      {
        await this.showColumns( this.query );
      }

    	return this.connector.select( this.query ).then( ( result ) =>
      {
        let set_elapsed_time = process.hrtime(set_start_time);
        result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

        return result;
      });
    }
    else{ return { ok : false, error: { code: 'UNDEFINED_TABLE' } }; }
	}

  async get_all_query( columns = '*', data = null, alias = null )
  {
    if( this.query.table || ( this.query.union && this.query.union.length ) )
    {
      this.query.operation = 'select';
      this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data };

      if( this.query.group_by )
      {
        await this.showColumns( this.query );
      }

      if( alias )
      {
        return '( ' + this.connector.build( this.query ) + ' ) ' + alias + ' ';
      }
      else
      {
        return this.connector.build( this.query );
      }
    }
    else{ return { ok : false, error: { code: 'UNDEFINED_TABLE' } }; }
  }

	get_all( columns = '*', data = null )
	{
    let set_start_time = process.hrtime();
    if( this.query.table || ( this.query.union && this.query.union.length ) )
    {
      return new TimedPromise( async( resolve, reject, remaining_ms ) =>
      {
    		this.query.operation = 'select';
    		this.query.columns = { columns: ( Array.isArray( columns ) ? columns.join(',') : columns ), data: data } ;

        if( this.query.group_by )
        {
          await this.showColumns( this.query );
        }

  			return this.connector.select( this.query ).then( ( result ) =>
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
  			});
      });
    }
    else{ return { ok : false, error: { code: 'UNDEFINED_TABLE' } }; }
	}

  showColumns( query )
  {
    return new TimedPromise( async( resolve, reject, remaining_ms ) =>
    {
      if( query.columns && query.columns.columns.indexOf( '*' ) !== -1 )
      {
        let all_tables = false, columns_by_tables = {};
        let tables_columns = this.connector.get_tables_columns( query.columns.columns );

        if( query.columns.columns === '*' || tables_columns.indexOf('*') !== -1 ){ all_tables = true; }

        let [ table, alias ] = query.table.split(/\s+/i);
        let word = ( alias ? alias : table );

        if( all_tables || ( tables_columns && tables_columns.indexOf( word ) !== -1 ) )
        {
          if( !this.tables || ( this.tables && !this.tables.hasOwnProperty( table ) ) )
          {
            await this.describe_table( 'columns', table );
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
                    await this.describe_table( 'columns', table );
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

	delete()
	{
    let set_start_time = process.hrtime();
    if( this.query.table )
    {
      this.query.operation = 'delete';

  		return this.connector.delete( this.query ).then( ( result ) =>
      {
        let set_elapsed_time = process.hrtime(set_start_time);
        result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;
        return result;
      });
    }
    else{ return { ok : false, error: { code: 'UNDEFINED_TABLE' } }; }
	}

  set( data )
  {
    let set_start_time = process.hrtime();
    if( this.query.table )
    {
      if( data && ( Array.isArray( data ) ? data.length : typeof data === 'object' ) )
      {
        if( !Array.isArray( data ) ){ data = [ data ]; }

        return this._get_existing_rows( data ).then( async( existing ) =>
        {
          let autoIncrementColumn = null;

          if( this.tables && this.tables[this.query.table].columns )
          {
            for( let columnName in this.tables[this.query.table].columns )
            {
              if( this.tables[this.query.table].columns.hasOwnProperty( columnName ) && this.tables[this.query.table].columns[columnName][ 'increment'] ){ autoIncrementColumn = columnName; break; }
            }
          }

          if( existing.ok )
          {
            var result = { ok: true, error: null, affected_rows: 0, changed_rows:  0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], sql_time: existing.sql_time };
            let insert_data = [], update_data = [], update_columns = [], changed_ids = [];

            for( var i = 0; i < data.length; ++i )
            {
              var datum = Object.keys(data[i]).filter(column =>  this.tables[this.query.table].columns.hasOwnProperty(column.replace(/^[&!?]+/,''))).reduce((obj, column) => { obj[column] = data[i][column]; return obj; }, {});
                  datum = this._create_datum( datum, existing.rows[i], update_columns );

              if( typeof existing.rows[i] === 'undefined' )
              {
                if( autoIncrementColumn && datum.hasOwnProperty( autoIncrementColumn ) ) { delete datum[ autoIncrementColumn ]; }

                insert_data.push(datum);
              }
              else if( datum )
              {
                update_data.push(datum);

                if( typeof datum.__primary__ != 'undefined' )
                {
                    changed_ids.push(datum.__primary__);
                }
              }
              else if( existing.rows[i] ) { result.affected_rows++ }
            }

            if( insert_data.length )
            {
              var inserted = await this._subquery( this.query.table ).insert( insert_data );

              result.sql_time += inserted.sql_time;

              if( inserted.ok )
              {
                result.affected_rows  += inserted.affected_rows;
                result.changed_rows   += inserted.changed_rows;
                result.inserted_id    = result.inserted_id || inserted.inserted_id;
                result.inserted_ids   = result.inserted_ids.concat( inserted.inserted_ids );;
                result.changed_id     = result.changed_id || inserted.changed_id;
                result.changed_ids    = result.changed_ids.concat( inserted.changed_ids );
                result.row            = result.row || inserted.row;
                result.rows           = result.rows.concat( inserted.rows );
              }
              else{ return { ok: false, error: inserted.error } }
            }

            if( update_data.length && update_columns.length )
            {
              var update_set = update_columns;
              var updated = await this._subquery( this.query.table ).update( update_set, update_data );
              result.sql_time += updated.sql_time;

              if( updated.ok )
              {
                  result.affected_rows  += updated.affected_rows;
                  result.changed_rows   += updated.changed_rows;
                  result.inserted_id    = result.inserted_id || updated.inserted_id;
                  result.inserted_ids   = result.inserted_ids.concat( updated.inserted_ids );;
                  result.changed_id     = result.changed_id || updated.changed_id;
                  result.changed_ids    = result.changed_ids.concat( updated.changed_ids );
                  result.row            = result.row || updated.row;
                  result.rows           = result.rows.concat( updated.rows );

                  for( let changed_id of changed_ids )
                  {
                    if( result.changed_ids.indexOf(changed_id) === -1 )
                    {
                      result.changed_ids.push(changed_id);
                    }
                  }
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
    let set_start_time = process.hrtime();

    if( this.query.table )
    {
      if( data && ( Array.isArray( data ) ? data.length : typeof data == 'object' ) )
      {
        if( !Array.isArray( data ) ){ data = [ data ]; }

        this.query.operation = 'insert';
        this.query.data = data;
        this.query.columns = this._get_all_columns( data );

        if( ignore ){  this.query.options.push('ignore'); }

        return this.connector.insert( this.query).then( async ( result ) =>
        {

      			if( result.ok && result.changed_rows > result.changed_ids.length )
      			{
              if( !this.tables || !this.tables.hasOwnProperty( this.query.table ) ){ await this.describe_table('indexes'); }
      				let indexes = this._get_main_indexes(); // TODO ked index neni primary jeden stlpec tak do changed_ids dat objekt s nazvami stlpcov a hodnotami

      				if( indexes )
      				{
      					var inserted_ids = [], changed_ids = [];

      					for( var i = 0; i < this.query.data.length; ++i )
      					{
      						var index = null, successful = true;

      						for( var j = 0; j < indexes.length; ++j )
      						{
      							if( typeof this.query.data[i][indexes[j]] != 'undefined' )
      							{
      								index = ( index === null ? this.query.data[i][indexes[j]] : index + '_' + this.query.data[i][indexes[j]] );
      							}
      							else{ successful = false; break; }
      						}

      						if( successful )
      						{
      							inserted_ids.push(index);
      							changed_ids.push(index);
      						}
      						else{ break; }
      					}

      					result.inserted_id = result.changed_id = ( inserted_ids.length ? inserted_ids[0] : null );
      					result.inserted_ids = inserted_ids;
      					result.changed_ids = changed_ids;
      				}
      			}

            let set_elapsed_time = process.hrtime(set_start_time);
            result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

      			return result;
        });
      }
      else
      {
        let set_elapsed_time = process.hrtime( set_start_time );
        return new TimedPromise(( resolve ) => { resolve( { ok: true, error: null, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], time: set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000 }) });
      }
    }
    else
    {
      return new TimedPromise(( resolve ) => { resolve( { ok: false, error: { code: 'UNDEFINED_TABLE' }, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] } ) });
    }
  }

  async update( set, data = null ) // TODO v set metode ak stlpec pre data nie je definovany tak dat len nazonv stlpca WHEN condition THEN COLUMN - poskusat ci pojde vsade
  {
    let set_start_time = process.hrtime();
    if( this.query.table )
    {
      /* input
          1) a = :a, b = :b		{ a: 1, b: 2 }
          2) a,b					[{ id: 1, a: 1, b: 2 }, { id:2, a: 4 }, { id:2, b: 5 }, { id:3, b: 4. __indexes__: ['id'] }];  // momentalne iba z metody set moze prist taketo pole, inak da error
          3) [ ...columns ]       [{ id: 1, a: 1, b: 2 }, { id:2, a: 4 }, { id:2, b: 5 }, { id:3, b: 4. __indexes__: ['id'] }];  // momentalne iba z metody set moze prist taketo pole, inak da error
       */

      // TODO ochrany nech hned ukonci ked je prazdny update a ked je set cez CASE tak spravit z data pole ak nie je

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

        if( data && Array.isArray(data) )
        {
          if( !this.tables )
          {
            let described = await this.describe_table('indexes');
            if( !described.ok ){ error = described.error; }
          }

          if( this.tables )
          {
            this.query.set = { indexes: this._get_all_indexes(), columns: Array.isArray(this.query.set) ? this.query.set : this.query.set.split(/\s*,\s*/) };

            if( this.query.where )
            {
              this.query.update_with_where = true;
            }
            else
            {
              let where = this._generate_where_for_indexes( this.query.set.indexes, data );
              this.where( where.condition, where.data );
            }

            if( !this.query.set.indexes.length && !this.query.where ){ error = new SQLError( 'INVALID_ENTRY' ).get(); }
          }
        }

        if( !error )
        {
          return this.connector.update( this.query ).then( ( result ) =>
          {
      			if( result.ok && result.changed_rows > result.changed_ids.length )
      			{
      				let indexes = this._get_main_indexes(); // TODO ked index neni primary jeden stlpec tak do changed_ids dat objekt s nazvami stlpcov a hodnotami

      				if( indexes )
      				{
      					let changed_ids = [];

      					if( this.query.data )
      					{
      						for( var i = 0; i < this.query.data.length; ++i )
      						{
      							var index = null, successful = true;

      							for( var j = 0; j < indexes.length; ++j )
      							{
      								if( typeof this.query.data[i][indexes[j]] !== 'undefined' )
      								{
      									index = ( index === null ? this.query.data[i][indexes[j]] : index + '_' + this.query.data[i][indexes[j]] );
      								}
      								else{ successful = false; break; }
      							}

      							if( successful )
      							{
      								changed_ids.push(index);
      							}
      							else{ break; }
      						}
      					}

      					result.changed_id = ( changed_ids.length ? changed_ids[0] : null );
      					result.changed_ids = changed_ids;
      				}
      			}

            let set_elapsed_time = process.hrtime(set_start_time);
            result.time = set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000;

      			return result;
          });
        }
        else
        {
          return new TimedPromise(( resolve ) => { resolve( { ok: false, error: error, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] } ) });
        }
      }
      else
      {
        let set_elapsed_time = process.hrtime(set_start_time);
        return new TimedPromise(( resolve ) => { resolve( { ok: true, error: null, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [], time: set_elapsed_time[0] * 1000 + set_elapsed_time[1] / 1000000 }) });
      }
    }
    else
    {
      return new TimedPromise(( resolve ) => { resolve( { ok: false, error: { code: 'UNDEFINED_TABLE' }, affected_rows: 0, changed_rows: 0, inserted_id: null, inserted_ids: [], changed_id: null, changed_ids: [], row: null, rows: [] } ) });
    }
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

      if( !existing_row || value != existing_value || ( existing_row.__indexes__ &&  existing_row.__indexes__.indexOf(column) !== -1 ) )
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

  _get_main_indexes()
  {
    if( this.tables )
    {
      if( this.tables[this.query.table] && this.tables[this.query.table].indexes )
      {
        if( this.tables[this.query.table].indexes.primary )
        {
          if( typeof this.tables[this.query.table].indexes.primary == 'string' )
          {
            return this.tables[this.query.table].indexes.primary.split(/\s*,\s*/);
          }
        }

        if( this.tables[this.query.table].indexes.unique )
        {
          if( typeof this.tables[this.query.table].indexes.unique == 'string' )
          {
            return this.tables[this.query.table].indexes.unique.split(/\s*,\s*/);
          }
          else if( this.tables[this.query.table].indexes.unique.length )
          {
            return this.tables[this.query.table].indexes.unique[0].split(/\s*,\s*/);
          }
        }
      }
    }

    return null;
  }

	_get_all_indexes()
	{
		var indexes = [];

		if( this.tables )
		{
			if( this.tables[this.query.table] && this.tables[this.query.table].indexes )
			{
				if( this.tables[this.query.table].indexes.primary )
				{
					if( typeof this.tables[this.query.table].indexes.primary == 'string' )
					{
						indexes.push( this.tables[this.query.table].indexes.primary.split(/\s*,\s*/) );
					}
				}

				if( this.tables[this.query.table].indexes.unique )
				{
					if( typeof this.tables[this.query.table].indexes.unique == 'string' )
					{
						indexes.push( this.tables[this.query.table].indexes.unique.split(/\s*,\s*/) );
					}
					else for( var i = 0; i < this.tables[this.query.table].indexes.unique.length; ++i )
					{
						indexes.push( this.tables[this.query.table].indexes.unique[i].split(/\s*,\s*/) );
					}
				}
			}

			return indexes;
		}
	}

	_get_used_indexes( data )
	{
		var indexes = [], indexes_groups = [], indexes_order = [], indexes_groups_iterators = [], indexes_unique_groups = [];

		if( this.tables[this.query.table] && this.tables[this.query.table].indexes )
		{
			if( this.tables[this.query.table].indexes.primary )
			{
				if( typeof this.tables[this.query.table].indexes.primary == 'string' )
				{
					indexes.push( this.tables[this.query.table].indexes.primary.split(/\s*,\s*/) );
				}
			}

			if( this.tables[this.query.table].indexes.unique )
			{
				if( typeof this.tables[this.query.table].indexes.unique == 'string' )
				{
					indexes.push( this.tables[this.query.table].indexes.unique.split(/\s*,\s*/) );
				}
				else for( var i = 0; i < this.tables[this.query.table].indexes.unique.length; ++i )
				{
					indexes.push( this.tables[this.query.table].indexes.unique[i].split(/\s*,\s*/) );
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

	_get_all_existing_columns( data, columns = [] )
	{
		columns = [];
		let table_column = this.tables[this.query.table].columns;

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

		if( this.tables[this.query.table].indexes.primary )
		{
			let primary = this.tables[this.query.table].indexes.primary.split(/\s*,\s*/);

			if( primary.length == 1 && columns.indexOf( primary[0] ) === -1 )
			{
				columns.push( primary[0] );
			}
		}

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

		return { condition: ( conditions.length ? '( ' + conditions.join(' ) OR ( ') +  ' )' : '' ), data: condition_data, index: data_index_to_i  };
	}

	_get_existing_rows( data )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			if( !this.tables ){ await this.describe_table(); }

			if( this.tables )
			{
				var indexes = this._get_used_indexes( data );
				var rows_index = {}, time = 0;

				for( let index in indexes )
				{
					var existing = await this._get_existing_rows_for_index( data,  index.split(','), indexes[index], rows_index ).timeout( remaining_ms ).catch( (e, remaining_ms ) => e );
					time +=  existing.time;

					if( existing.error ){ resolve({  ok: false, error: existing.error }); }
				}

				resolve({ ok: true, rows: rows_index, sql_time: time });
			}
			else{ resolve({  ok: false, error: { code: 'UNDEFINED_TABLE' } }); }
		});
	}

	_get_existing_rows_for_index( data, indexes, data_indexes, rows_index )
	{
		var columns = this._get_all_existing_columns( data, indexes );
		var where = this._generate_where_for_indexes( [ indexes ], data, data_indexes );

		return this._subquery( this.query.table ).where( where.condition, where.data ).get_all( columns ).then( ( rows ) =>
		{
			if( rows.ok )
			{
				let primary = ( this.tables[this.query.table].indexes.primary ? this.tables[this.query.table].indexes.primary.split(/\s*,\s*/) : null );
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

	describe_table( option = null, table = null )
	{
		return new TimedPromise( async( resolve, reject, remaining_ms ) =>
		{
			let table_to_describe = ( table ? table : this.query.table );
			let table_columns = null, table_indexes = null;
			if( !option || option === 'columns' )
			{
				table_columns = await this.connector.describe_columns( table_to_describe );
			}

			if( !option || option === 'indexes' )
			{
				table_indexes = await this.connector.show_table_index( table_to_describe );
			}

			if( table_columns && !table_columns.ok ) { resolve({  ok: false, error: table_columns.error }); }
			else if( table_indexes && !table_indexes.ok ){ resolve({  ok: false, error: table_indexes.error }); }
			else
			{
				if( !this.tables ){ this.tables = {}; }
				this.tables[table_to_describe] = { columns: ( table_columns ? table_columns.columns : null ), indexes: ( table_indexes ? table_indexes.indexes : null ) };
				resolve({ ok: true });
			}
		});
	}

	create_table( execute )
	{
		let query = this.connector.create_table( this.query.table, this.query.alias );

		if( execute )
		{
			return new TimedPromise( async( resolve, reject, remaining_ms ) =>
			{
				resolve( await this._subquery( query ).execute() );
			});
		}
		else { return query; }
	}

	drop_table( execute )
	{
		let query = this.connector.drop_table( this.query.table );

		if( execute )
		{
			return new TimedPromise( async( resolve, reject, remaining_ms ) =>
			{
			resolve( await this._subquery( query ).execute());
			});
		}
		else { return query; }
	}
}