'use strict';

function is_char_escaped( str, position )
{
	var escaped = false;

	while( --position >= 0 && str[position] == '\\' ){ escaped = !escaped; }

	return escaped;
}

function isOf( characters, character )
{
	return ( characters.indexOf(character) > -1 );
}

module.exports =
{
	expand: function( data, escape )
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
	},

	expand_values: function( query, data, escape_value, escape_column = null )
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
	},

	order_with_group: function( order_by, group_by, is_agregated )
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
	},

	escape_columns: function( query, escape_column, transform_functions )
	{
		if( typeof query === 'string')
		{
			query = this.escape_functions( query, transform_functions );

			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
			keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL', 'ROWS', 'OFFSET', 'FETCH', 'NEXT', 'ONLY', 'ROWNUM', 'SYSTIMESTAMP' ],
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
	},

	escape_functions: function( query, transform_functions )
	{
		if( typeof query === 'string')
		{
			let is_function = false, function_string = '', quoted_word = '', function_end = false, functions = {}, check_function_end = false;
			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:', function_delimiters = ',',
			keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL', 'ROWS', 'OFFSET', 'FETCH', 'NEXT', 'ONLY', 'ROWNUM', 'SYSTIMESTAMP' ],
			uppercase_keywords = [ 'END' ],
			brackets = '()', quotes = '"\'`', quoted = false, brackets_count = 0;

			for( var i = 0; i <= query.length; ++i )
			{
				if( is_function ){ function_string += query[i]; }

				if( i < query.length && ( quoted || isOf( quotes, query[i] ) ) )
				{
					if( is_function )
					{
						quoted_word += query[i];

						if( isOf( quotes, query[i] ) )
						{
							quoted = ( quoted ? ( ( quoted == query[i] && !is_char_escaped( query, i ) ) ? false : quoted ) : query[i] );
							if( !quoted && is_function )
							{
								functions[Object.keys( functions ).length - 1 ].values.push( quoted_word );
								quoted_word = '';
							}
						}
					}
				}
				else if( i + 1 < query.length && query[i] == '-' && word && query[i-1].match(/[a-z]/) && query[i+1].match(/[a-z]/) )
				{
					word += query[i];
				}
				else if( i < query.length && ( !isOf( delimiters, query[i] ) || ( is_function && !isOf( function_delimiters, query[i] ) ) ) && !isOf( brackets, query[i] ) ) // agregating word
				{
					word += query[i];
				}
				else if( word )
				{
					word = word.trim();
					if( word.length && is_function && word != '.' && ( i <= word.length || query[i-word.length-1] != ':' ) && query[i] != '(' && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						functions[Object.keys( functions ).length - 1].values.push( word );
					}
					else if( query[i] === '(' && word )
					{
						if( !brackets_count ){ function_string = word+query[i]; }

						is_function = true;
						functions[[Object.keys( functions ).length]] = { name: word, values: [] };
					}

					if( query[i] === '(' && is_function ){ brackets_count++; }
					if( query[i] === ')' ){ check_function_end = true; }

					word = '';
				}
				else if( query[i] === ')' ){ check_function_end = true; }

				if( check_function_end )
				{
					check_function_end = false;
					if( brackets_count && ( brackets_count - 1 ) === 0 ){ function_end = true; }
					if( brackets_count ){ brackets_count-- };

					if( functions.hasOwnProperty( Object.keys( functions ).length - 2 ))
					{
						let transformed = transform_functions( functions, ( Object.keys( functions ).length - 1 ));

						if( transformed )
						{
							if( Array.isArray( transformed ) )
							{
								functions[[ Object.keys( functions ).length - 2 ]].values = functions[[ Object.keys( functions ).length - 2 ]].values.concat( transformed );
							}
							else { functions[[ Object.keys( functions ).length - 2 ]].values.push( transformed ); }
						}
						else if( functions[[ Object.keys( functions ).length - 1 ]].values.length )
						{
							functions[[ Object.keys( functions ).length - 2 ]].values.push( functions[[ Object.keys( functions ).length - 1 ]].name + '('+functions[[ Object.keys( functions ).length - 1 ]].values.join(' ') + ')' ); 
						}

						delete functions[[ Object.keys( functions ).length - 1 ]];
					}
				}

				if( function_end )
				{
					function_end = false; is_function = false;
					let transformed = transform_functions( functions, Object.keys( functions ).length - 1 );

					if( transformed )
					{
						query = query.substr(0, i - function_string.length) + transformed + query.substr(i+1);
						i += transformed.length - function_string.length;
					}

					functions = {}; function_string = '';
				}
			}
		}

		return query;
	},

	get_tables_columns: function( query )
	{
		let tables = [];
		if( typeof query === 'string')
		{
			var word = '', delimiters = ' \t\r\n,.=+-!<>/%&|^~:',
			keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL', 'ROWS', 'OFFSET', 'FETCH', 'NEXT', 'ONLY', 'ROWNUM', 'SYSTIMESTAMP' ],
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
						if( isOf( quotes, word[0].trim() ) && isOf( quotes, word[ word.length-1 ].trim() ) )
						{
							unescaped_word = word.substr( 1, word.length-2 );
							query = query.substr(0, i - word.length ) + unescaped_word + query.substr(i);
							i += escaped_word.length - word.length;

							word = unescaped_word;
						}

						tables.push( word );
					}

					word = '';
				}
			}
		}

		return tables;
	},

	get_used_columns: function( query, alias = null, used_columns )
	{
		if( typeof query === 'string')
		{
			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
			keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL', 'ROWS', 'OFFSET', 'FETCH', 'NEXT', 'ONLY', 'ROWNUM', 'SYSTIMESTAMP' ],
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
};
