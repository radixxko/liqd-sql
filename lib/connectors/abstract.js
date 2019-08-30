'use strict';
const abstract_keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL', 'ROWS', 'OFFSET', 'FETCH', 'NEXT', 'ONLY', 'ROWNUM', 'SYSTIMESTAMP', 'REGEXP', 'UTF8', 'RLIKE', 'AGAINST', 'MATCH', 'BOOLEAN', 'MODE' ];
var escape_keyword = '';

function is_char_escaped( str, position )
{
	var escaped = false;

	while( --position >= 0 && str[position] == escape_keyword ){ escaped = !escaped; }

	return escaped;
}

function isOf( characters, character )
{
	return ( characters.indexOf(character) > -1 );
}

module.exports = function( config, emit )
{
	return new( function()
	{
		const Abstract_test = this;
		escape_keyword = config.escape_keyword;

	this.expand = function( data, escape )
	{
		if( Array.isArray(data) )
		{
			let value = '';

			for( let i = 0; i < data.length; ++i )
			{
				value += ( value ? ',' : '' ) + escape( data[i] );
			}

			return value;
		}
		else{ return escape(data); }
	}

	this.expand_values = function( query, data, escape_value, escape_column = null )
	{
		return query.replace( /:[0-9a-zA-Z\-_\?]+/g, function( match )
		{
			var variable = match.substr(1);
			if( variable == '?' )
			{
				if( data && typeof data == 'object' )
				{
					if( Array.isArray(data) )
					{
						var value = '';

						for( var i = 0; i < data.length; ++i )
						{
							value += ( value ? ',' : '' ) + escape_value( data[i] );
						}

						return '(' + value + ')';
					}
					else if( data instanceof Buffer )
					{
						return escape_value( data );
					}
					else
					{
						var keys = [];
						var values = [];

						for( var key in data )
						{
							if( key.substr(0,2) == '__' && key.substr(-2) == '__' ){ continue; }

							keys.push( escape_column ? escape_column( key ) : key );
							values.push( escape_value( data[key] ) );
						}

						return '(' + keys.join( ',' ) + ') VALUES (' + values.join( ',' ) + ')';
					}
				}
				else
				{
					return escape_value( data );
				}
			}
			else if( data[variable] && Array.isArray(data[variable]) )
			{
				var value = '';

				for( var i = 0; i < data[variable].length; ++i )
				{
					value += ( value ? ',' : '' ) + escape_value( data[variable][i] );
				}

				return value;
			}
			else if( typeof data[variable] == 'undefined' )
			{
				return match;
			}
			else if( typeof data[variable] == 'number' )
			{
				return data[variable];
			}
			else
			{
				return escape_value( data[variable] );
			}
		});
	}

	this.order_with_group = function( order_by, group_by, is_agregated )
	{
		if( ( group_by || is_agregated ) && order_by && order_by.hasOwnProperty( 'condition' ) && typeof order_by.condition === 'string' )
		{
			let columns_in_group = [], order_string = order_by.condition;

			if( typeof group_by !== 'undefined' ) { group_by.condition.split(',').forEach( ( part ) => { if(part){ columns_in_group.push( part.trim() ); } }); }

			order_by.condition.split(',').forEach( ( order ) => { let orderer = order.replace(/(ASC|DESC)/g, '' ).trim(); if( !columns_in_group.includes( orderer ) ){ columns_in_group.push( orderer ); } });

			if( !group_by || !group_by.hasOwnProperty( 'condition' ) ) { group_by = { condition: null, data: null }; }

			group_by.condition = columns_in_group.join( ', ' );
		}

		return group_by;
	}

	this.escape_columns = function( query, escape_column, transform_functions )
	{
		if( typeof query === 'string')
		{
			if( transform_functions ){ query = this.escape_functions( query, transform_functions ); }else{ }

			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
			keywords = abstract_keywords,
			uppercase_keywords = [ 'END' ],
			brackets = '()', quotes = '"\'`', quoted = false;

			for( var i = 0; i <= query.length; ++i )
			{
				if( i < query.length && ( quoted || isOf( quotes, query[i] ) ) ) // string constant
				{
					if( isOf( quotes, query[i] ) )
					{
						quoted = ( quoted ? ( ( quoted == query[i] && !is_char_escaped( query, i ) ) ? false : quoted ) : query[i] );
					}
				}
				else if( i + 1 < query.length && query[i] == '-' && word && query[i-1].match(/[a-z]/) && query[i+1].match(/[a-z]/) )
				{
					word += query[i];
				}
				else if( i < query.length && !isOf( delimiters, query[i] ) && !isOf( brackets, query[i] ) ) // agregating word
				{
					word += query[i];
				}
				else if( word )
				{
					if( word != '.' && ( i <= word.length || query[i-word.length-1] != ':' ) && query[i] != '(' &&  isNaN(word) && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						var escaped_word = word.split('.').map(escape_column).join('.');

						query = query.substr(0, i - word.length) + escaped_word + query.substr(i);
						i += escaped_word.length - word.length;
					}

					word = '';
				}
			}
		}

		return query;
	}

	this.escape_functions = function( query, transform_functions )
	{
		if( typeof query === 'string')
		{
			let is_bracketed = false, function_string = '', quoted_word = '', function_end = false, functions = {}, check_function_end = false, multi_f = false;
			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:', function_delimiters = ',',
			keywords = abstract_keywords, brackets_counter = {},
			uppercase_keywords = [ 'END' ],
			brackets = '()', quotes = '"\'`', quoted = false, brackets_count = 0;

			for( var i = 0; i <= query.length; ++i )
			{
				if( is_bracketed ){ function_string += query[i]; }

				if( i < query.length && ( quoted || isOf( quotes, query[i] ) ) )
				{
					if( is_bracketed )
					{
						quoted_word += query[i];

						if( isOf( quotes, query[i] ) )
						{
							quoted = ( quoted ? ( ( quoted == query[i] && !is_char_escaped( query, i ) ) ? false : quoted ) : query[i] );
							if( !quoted && is_bracketed )
							{
								word += quoted_word;
								quoted_word = '';
							}
						}
					}
				}
				else if( i + 1 < query.length && query[i] == '-' && word && query[i-1].match(/[a-z]/) && query[i+1].match(/[a-z]/) )
				{
					word += query[i];
				}
				else if( i < query.length && ( !isOf( delimiters, query[i] ) || ( is_bracketed && !isOf( function_delimiters, query[i] ) ) ) && !isOf( brackets, query[i] ) ) // agregating word
				{
					word += query[i];
				}
				else if( query[i] === '(' && !word )
				{
					if( !brackets_count ){ function_string = query[i]; }

					brackets_count++;

					is_bracketed = true;
					functions[[Object.keys( functions ).length]] = { name: '', values: [], prefix: '' };
				}
				else if( word )
				{
					//if( is_bracketed && query[i] === function_delimiters ){ functions[ Object.keys( functions ).length -1 ].next_value = true; }

					word = word.trim();
					if( word.length && is_bracketed && word != '.' && ( i <= word.length || query[i-word.length-1] != ':' ) && query[i] != '(' && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						if( !functions[Object.keys( functions ).length - 1].next_value && functions[Object.keys( functions ).length - 1].values.length )
						{
							functions[Object.keys( functions ).length - 1].values[ functions[Object.keys( functions ).length - 1].values.length - 1 ] += word;
						}
						else { functions[Object.keys( functions ).length - 1].values.push( word ); }
					}
					else if( query[i] === '(' )
					{
						if( !brackets_count ){ function_string = word+query[i]; }

						is_bracketed = true;
						let function_name = word.split( ' ' ).pop();

						functions[[Object.keys( functions ).length]] = { name: function_name, values: [], prefix: word.substr( 0, word.length - function_name.length ), next_value: false };
					}

					if( query[i] === '(' && is_bracketed )
					{
						brackets_count++;
					}


					if( query[i] === ')' ){ check_function_end = true; }
					if( is_bracketed && query[i] === function_delimiters ){ functions[ Object.keys( functions ).length -1 ].next_value = true; }

					word = '';
				}
				else if( query[i] === ')' ){ check_function_end = true; }
				else if( query[i] === ',' )
				{
					if( is_bracketed && query[i] === function_delimiters && functions[ Object.keys( functions ).length -1 ] ){ functions[ Object.keys( functions ).length -1 ].next_value = true; }
				}

				if( check_function_end )
				{
					check_function_end = false;

					if( brackets_count ){ brackets_count-- };

					if( functions.hasOwnProperty( Object.keys( functions ).length - 1 ))
					{
						let parent = Object.keys( functions ).length - 2, current = Object.keys( functions ).length - 1;

						if( parent >= 0 )
						{
							let transformed = transform_functions( functions, current );

							if( transformed )
							{
								multi_f = true;
								if( Array.isArray( transformed ) )
								{
									functions[ parent ].values = functions[ parent ].values.concat( transformed );
								}
								else
								{
									if( !functions[ parent ].next_value && functions[ parent ].values.length )
									{
										functions[ parent ].values[ functions[ parent ].values.length -1 ] = functions[ parent ].values[ functions[ parent ].values.length -1 ] + functions[ current ].prefix + transformed;
									}
									else
									{
										functions[ parent ].values.push( functions[ current ].prefix + transformed );
									}
								}
							}
							else if( functions[ current ].values.length )
							{
								if( !functions[ parent ].next_value && functions[ parent ].values.length )
								{
									functions[ parent ].values[ functions[ parent ].values.length -1 ] = functions[ parent ].values[ functions[ parent ].values.length -1 ] + ( functions[ current ].prefix + functions[ current ].name ? functions[ current ].prefix + functions[ current ].name + ' ' : '' ) + '( '+functions[ current ].values.join(' ') + ' ) ';
								}
								else
								{
									functions[ parent ].values.push( ( functions[ current ].prefix + functions[ current ].name ? functions[ current ].prefix + functions[ current ].name + ' ' : '' ) + '( '+functions[ current ].values.join(' ') + ' ) ' );
								}
							}

							delete functions[ current ];
						}
						else if( !brackets_count ) { function_end = true; }
					}
				}

				if( function_end )
				{
					function_end = false; is_bracketed = false;
					let transformed = transform_functions( functions, Object.keys( functions ).length - 1 );

					/* istanbul ignore else  */
					if( transformed )
					{
						transformed = functions[ Object.keys( functions ).length - 1 ].prefix + transformed;
						query = query.substr(0, i - function_string.length) + transformed + query.substr(i+1);
						i += transformed.length - function_string.length;
					}
					else if( multi_f && functions[ Object.keys( functions ).length - 1 ].values.length )
					{
						transformed = ' '+ functions[ Object.keys( functions ).length - 1 ].prefix + functions[ Object.keys( functions ).length - 1 ].name + '('+ functions[ Object.keys( functions ).length - 1 ].values.join(' ') +') ';
						query = query.substr(0, i - function_string.length) + transformed + query.substr(i+1);
						i += transformed.length - function_string.length;
					}

					functions = {}; function_string = '';
				}
			}
		}

		return query;
	}

	this.get_tables_columns = function( query )
	{
		let tables = [];
		if( typeof query === 'string')
		{
			var word = '', delimiters = ' \t\r\n,.=+-!<>/%&|^~:',
			keywords = abstract_keywords,
			uppercase_keywords = [ 'END' ],
			brackets = '()', quotes = '"\'`', quoted = false;

			for( var i = 0; i <= query.length; ++i )
			{
				if( i + 1 < query.length && query[i] == '-' && word && query[i-1].match(/[a-z]/) && query[i+1].match(/[a-z]/) )
				{
					word += query[i];
				}
				else if( i < query.length && !isOf( delimiters, query[i] ) && !isOf( brackets, query[i] ) )
				{
					word += query[i];
				}
				else if( word )
				{
					if( ( word != '.' && (query[i] + query[i+1]) === '.*' ) || ( word === '*' && query[i-2] !== '.' ) )
					{
						/* istanbul ignore if  */
						if( isOf( quotes, word[0].trim() ) && isOf( quotes, word[ word.length-1 ].trim() ) )
						{
							let unescaped_word = word.substr( 1, word.length-2 );
							query = query.substr(0, i - word.length ) + unescaped_word + query.substr(i);
							i += unescaped_word.length - word.length;

							word = unescaped_word;
						}

						tables.push( word );
					}

					word = '';
				}
			}
		}

		return tables;
	}

	this.get_used_columns = function( query, alias = null, used_columns )
	{
		if( typeof query === 'string')
		{
			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
			keywords = abstract_keywords,
			uppercase_keywords = [ 'END' ],
			brackets = '()', quotes = '"\'`', quoted = false, next = false;

			for( var i = 0; i <= query.length; ++i )
			{
				if( i < query.length && ( quoted || isOf( quotes, query[i] ) ) ) // string constant
				{
					if( isOf( quotes, query[i] ) )
					{
						quoted = ( quoted ? ( ( quoted == query[i] && !is_char_escaped( query, i ) ) ? false : quoted ) : query[i] );
					}
				}
				else if( i < query.length && next )
				{
					if( isOf( ',', query[i] ) )
					{
						next = false;
					}
				}
				else if( i + 1 < query.length && query[i] == '-' && word && query[i-1].match(/[a-z]/) && query[i+1].match(/[a-z]/) )
				{
					word += query[i];
				}
				else if( i < query.length && !isOf( delimiters, query[i] ) && !isOf( brackets, query[i] ) ) // agregating word
				{
					word += query[i];
				}
				else if( word )
				{
					if( word != '.' && ( i <= word.length || query[i-word.length-1] != ':' ) && query[i] != '(' &&  isNaN(word) && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						let word_split = word.split('.');

						if( alias && word_split[0] === alias && used_columns.indexOf( word_split[1] ) === -1 )
						{
							used_columns.push( word_split[1] );
						}
						else if( !alias && used_columns.indexOf( word ) === -1 ){ used_columns.push( word ); }

						next = true;
					}

					word = '';
				}
			}
		}

		return used_columns;
	}
	})();
};
