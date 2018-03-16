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

	order_with_group: function( order_by, group_by )
	{
		if( order_by && order_by.hasOwnProperty( 'condition' ) && typeof order_by.condition === 'string' )
		{
			let columns_in_group = [], order_string = order_by.condition;

			if( typeof group_by !== 'undefined' ) { columns_in_group = group_by.condition.split(','); }

			var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
				keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL' ],
				uppercase_keywords = [ 'END' ],
				brackets = '()', quotes = '"\'`', quoted = false;

			for( var i = 0; i <= order_string.length; ++i )
			{
				if( i < order_string.length && ( quoted || isOf( quotes, order_string[i] ) ) ) // string constant
				{
					if( isOf( quotes, order_string[i] ) )
					{
						quoted = ( quoted ? ( ( quoted == order_string[i] && !is_char_escaped( order_string, i ) ) ? false : quoted ) : order_string[i] );
					}
				}
				else if( i + 1 < order_string.length && order_string[i] == '-' && word && order_string[i-1].match(/[a-z]/) && order_string[i+1].match(/[a-z]/) )
				{
					word += order_string[i];
				}
				else if( i < order_string.length && !isOf( delimiters, order_string[i] ) && !isOf( brackets, order_string[i] ) ) // agregating word
				{
					word += order_string[i];
				}
				else if( word )
				{
					if( word != '.' && ( i <= word.length || order_string[i-word.length-1] != ':' ) && order_string[i] != '(' &&  isNaN(word) && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						if( columns_in_group.indexOf( word.trim() ) === -1 )
						{
							columns_in_group.push( word.trim() );
						}
					}

					word = '';
				}
			}

			if( !group_by || !group_by.hasOwnProperty( 'condition' ) ) { group_by = { condition: null, data: null }; }

			group_by.condition = columns_in_group.join( ', ' );
		}

		return { group_by: group_by };
	},

	escape_columns_group: function( columns, group_by, order_by, escape )
	{
		if(  typeof columns === 'string' && typeof group_by !== 'undefined' && group_by.hasOwnProperty('condition') && typeof group_by.condition === 'string' && group_by.condition.length )
		{
			let columns_in_group = group_by.condition.split(',');

			let word = '', alias_word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
				keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL' ],
				uppercase_keywords = [ 'END' ],
				aggregate_keywords = [ 'MAX', 'MIN' ],
				brackets = '()', quotes = '"\'`', quoted = false, bracket_count = 0, dont_agregate = false, bracketed = false, agregated = false, agregate_brackets = 0, agregate_delimiters = ',=+-!<>*/%&|^~:';

			for( let i = 0; i <= columns.length; ++i )
			{
				if( i < columns.length && ( quoted || isOf( quotes, columns[i] ) ) ) // string constant
				{
					if( isOf( quotes, columns[i] ) )
					{
						quoted = ( quoted ? ( ( quoted == columns[i] && !is_char_escaped( columns, i ) ) ? false : quoted ) : columns[i] );
					}
				}
				else if( i + 1 < columns.length && columns[i] == '-' && word && columns[i-1].match(/[a-z]/) && columns[i+1].match(/[a-z]/) )
				{
					word += columns[i];
				}
				else if( i < columns.length && !isOf( delimiters, columns[i] ) && !isOf( brackets, columns[i] ) ) // agregating word
				{
					word += columns[i];
				}
				else if( word )
				{
					if( !dont_agregate && !agregated && columns_in_group.indexOf( word ) === -1 && word != '.' && ( i <= word.length || columns[i-word.length-1] != ':' ) && columns[i] != '(' &&  isNaN(word) && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						let escaped_word = word;

						if( escape !== '' )
						{
							escaped_word = escape + '( '+ word +' )' ;
						}
						alias_word = word;
						agregated = true;

						columns = columns.substr(0, i - word.length) + escaped_word + columns.substr(i);
						i += escaped_word.length - word.length;
					}
					else if ( !isNaN(word) || columns[i-word.length-1] === ':' || columns_in_group.indexOf( word ) !== -1 && word != '.' && ( i <= word.length || columns[i-word.length-1] != ':' ) && columns[i] != '(' &&  isNaN(word) && !keywords.includes( ( uppercase_keywords.includes( word.toUpperCase() ) ? word : word.toUpperCase() ) ) )
					{
						dont_agregate = true;
					}

					if( agregated && alias_word !== word )
					{
						alias_word = '';
					}

					word = '';
				}

				if( agregated && !bracket_count && isOf( agregate_delimiters, columns[i] ) && alias_word && !dont_agregate && !bracketed )
				{
					let alias = (  alias_word.indexOf( '.' ) !== -1 ? ' '+alias_word.split( '.' )[1] : ' '+alias_word );

					alias_word = '';
					columns = columns.substr(0, i ) + alias + columns.substr(i);
					i += alias.length ;
				}

				if( isOf( agregate_delimiters, columns[i] ) )
				{
					dont_agregate = false;
				}

				if( agregated && isOf( agregate_delimiters, columns[i] ) )
				{
					agregated = false;
				}

				bracket_count = bracket_count + ( columns[i] === '(' ? 1 : ( columns[i] === ')' ? -1 : 0 ) );
				if( bracket_count )
				{
					bracketed = true;
				}
				else if( isOf( agregate_delimiters, columns[i] ) )
				{
					bracketed = false;
				}
			}
		}

		return columns;
	},

  escape_columns: function( query, escape_column )
  {
    if( typeof query === 'string')
    {
      var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
          keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL' ],
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

  get_used_columns: function( query, alias = null, used_columns )
  {
    if( typeof query === 'string')
    {
      var word = '', delimiters = ' \t\r\n,=+-!<>*/%&|^~:',
          keywords = [ 'IN', 'AND', 'OR', 'DIV', 'MOD', 'SEPARATOR', 'DISTINCT', 'BINARY', 'COLLATE', 'IS', 'LIKE', 'ORDER', 'LIMIT', 'SELECT', 'WHERE', 'ORDER', 'BY', 'GROUP', 'UPDATE', 'INSERT', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'TOP', 'FROM', 'SET', 'INTO', 'NOT', 'IS', 'XOR', 'NOT', 'ON', 'UNION', 'CASE', 'WHEN', 'ELSE', 'THEN', 'END', 'CURRENT_TIMESTAMP', 'NULL', 'AS', 'HAVING', 'MINUTE', 'NOW', 'INTERVAL', 'ASC', 'DESC', 'TRUE', 'FALSE', 'BETWEEN', 'MONTH', 'DUAL', 'ALL' ],
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
            let word_split = word.split('.');

            if( alias && word_split[0] === alias && used_columns.indexOf( word_split[1] ) === -1 )
            {
              used_columns.push( word_split[1] );
            }
            else if( !alias && used_columns.indexOf( word ) === -1 ){ used_columns.push( word ); }
          }

          word = '';
        }
      }
    }

    return used_columns;
  }
};
