# Node.JS SQL connector for MySQL, SQL Server and Oracle Databases

[![Version npm](https://img.shields.io/npm/v/liqd-sql.svg)](https://www.npmjs.com/package/liqd-sql)
[![NPM downloads](https://img.shields.io/npm/dm/liqd-sql.svg)](https://www.npmjs.com/package/liqd-sql)
[![Build Status](https://travis-ci.org/radixxko/liqd-sql.svg?branch=master)](https://travis-ci.org/radixxko/liqd-sql)
[![Coverage Status](https://coveralls.io/repos/github/radixxko/liqd-sql/badge.svg?branch=master)](https://coveralls.io/github/radixxko/liqd-sql?branch=master)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)


## Table of Contents

* [Installing](#installing)
* [Usage](#usage)
* [Create table](#create-table)
* [Select](#select)
* [Select query](#select-query)
* [Result](#result)
* [Join](#join)
* [Inner join](#inner-join)
* [Union](#union)
* [Where](#where)
* [Order by](#order-by)
* [Group by](#group-by)
* [Having](#having)
* [Limit](#limit)
* [Offset](#offset)
* [Execute](#execute)
* [Update](#update)
+ [with indexes](#update-with-indexes)
+ [with where and object ](#update-with-where)
+ [with where and string](#update-with-string)
* [Insert](#insert)
* [Set](#set)

## Installing

```
$ npm i liqd-sql
```

## Usage
```js
const SQL = new (require('liqd-sql'))(
{
	mysql :
	{
		host     : 'localhost',
		user     : 'root',
		password : '',
		database : 'test'
	}
});
```

## Create table

### SQL.query( config, table ).execute( execute )

- `config` {Object}
- `table` {String}
- `execute` {Boolean}

```js
await SQL.query({
	columns :
	{
		id      : { type: 'BIGINT:UNSIGNED', increment: true },
		name    : { type: 'VARCHAR:255' },
		surname : { type: 'VARCHAR:255' },
		cityID  : { type: 'BIGINT:UNSIGNED' }
	},
	indexes : {
		primary : 'id',
		unique  : [],
		index   : [ 'city' ]
	}
}, 'users' ).create_table( true );

await SQL.query({
	columns :
	{
		id   : { type: 'BIGINT:UNSIGNED', increment: true },
		name : { type: 'VARCHAR:255' }
	},
	indexes : {
		primary : 'id',
		unique  : [],
		index   : [ 'name' ]
	}
}, 'cities' ).create_table( true );
```

## Select

### .select_row( [columns = '*'[, data = null]] )

- `columns` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users' ).select_row();
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 0,
	inserted_id   : null,
	inserted_ids  : [],
	changed_id    : null,
	changed_ids   : [],
	row           : { id: 1, name: 'John', surname: 'D.', cityID: 1 },
	rows          : [ { id: 1, name: 'John', surname: 'D.', cityID: 1 } ],
	sql_time      : 1,
	time          : 1,
	query         : 'SELECT * FROM `users` LIMIT 1'
};
```

### .select( [columns = '*'[, data = null]] )

- `columns` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users' ).select();
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 2,
	changed_rows  : 0,
	inserted_id   : null,
	inserted_ids  : [],
	changed_id    : null,
	changed_ids   : [],
	row           : { id: 1, name: 'John', surname: 'D.', cityID: 1 },
	rows          : [ { id: 1, name: 'John', surname: 'D.', cityID: 1 }, { id: 2, name: 'Mark', surname: 'T.', cityID: 1 } ],
	sql_time      : 1,
	time          : 1,
	query         : 'SELECT * FROM `users`'
};
```

## Select query

### .select_row_query( [columns = '*'[, data = null[, alias = null]]] )

- `columns` {String}
- `data` {Any}
- `alias` {String}

```js
let data = await SQL.query( 'users' ).select_row_query();
```

Output
```
'SELECT * FROM `users` LIMIT 1'
```

### .select_query( [columns = '*'[, data = null[, alias = null]]] )

- `columns` {String}
- `data` {Any}
- `alias` {String}

```js
let data = await SQL.query( 'users' ).select_query();  
```

Output
```
SELECT * FROM `users`
```

## Result

### .select_row( [columns = '*'[, data = null]] )

```
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
	time          : 0,
	query         : ''
};
```

- `ok` {Boolean}
- `error` {Object}
- `affected_rows` {Number}
- `changed_rows` {Number}
- `inserted_id`
- `inserted_ids` {Array}
- `changed_id`
- `changed_ids` {Array}
- `row` {Object}
- `rows` {Array}
- `rows` {Array}
- `sql_time` {Number}
- `time` {Number}
- `query` {String}

## Join

### .join( table, condition[, data = null] )

- `table` {String}
- `condition` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users u' ).join( 'cities c', 'u.cityID = c.id' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` `u` LEFT JOIN `cities` `c` ON `u`.`cityID` = `c`.`id`
```

## Inner join

### .inner_join( table, condition[, data = null] )

- `table` {String}
- `condition` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users u' ).inner_join( 'cities c', 'u.cityID = c.id' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` `u` INNER JOIN `work` `w` ON `u`.`id` = `w`.`userID`
```

## Union

### .union( union )

- `union` {String|Array|Query}

## Where

### .where( condition[, data = null] )

- `condition` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users' ).where( ' id > 10 AND name = :?', 'John' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` WHERE `id` > 10 AND `name` = 'John'
```

```js
let data = await SQL.query( 'users' ).where( ' id > 10 ' ).where( 'name = :?', 'John' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` WHERE ( `id` > 10 ) AND ( `name` = 'John' )
```

## Order by

### .order_by( columns[, data = null] )

- `columns` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users' ).order_by( 'name ASC, surname DESC' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` ORDER BY `name` ASC, `surname` DESC
```


## Group by
- use one time

### .group_by( columns[, data = null] )

- `columns` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users' ).group_by( 'surname DESC' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` GROUP BY `surname`
```

## Having

### .having( condition[, data = null] )

- `condition` {String}
- `data` {Any}

```js
let data = await SQL.query( 'users' ).having( 'id > 3' ).select_query( '*' );
```

Output
```
SELECT * FROM `users` HAVING id > 3
```

## Limit

### .limit( limit )

- `limit` {Number}

```js
let data = await SQL.query( 'users' ).limit( 15 ).select_query( '*' );
```

Output
```
SELECT * FROM `users` LIMIT 15
```

## Offset

### .offset( offset )

- `offset` {Number}

```js
let data = await SQL.query( 'users' ).limit( 15 ).offset( 15 ).select_query( '*' );
```

Output
```
SELECT * FROM `users` LIMIT 15 OFFSET 15
```

## Execute

### .execute()

```js
let data = await SQL.query( 'SELECT * FROM users' ).execute();
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 2,
	changed_rows  : 0,
	inserted_id   : null,
	inserted_ids  : [],
	changed_id    : null,
	changed_ids   : [],
	row           : { id: 1, name: 'John', surname: 'D.', cityID: 1 },
	rows          : [ { id: 1, name: 'John', surname: 'D.', cityID: 1 }, { id: 2, name: 'Mark', surname: 'T.', cityID: 1 } ],
	sql_time      : 1,
	time          : 1,
	query         : 'SELECT * FROM `users`'
};
```

## Update

### .update( set[, data = null] )

- `set` {String|Array|Object}
- `data` {Any}

### Update with indexes
```js
let data = await SQL.query( 'users' ).update( { id: 1, name: 'Johnson' } );
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 1,
	inserted_id   : null,
	inserted_ids  : [],
	changed_id    : null,
	changed_ids   : [],
	row           : null,
	rows          : [],
	sql_time      : 1,
	time          : 1,
	query         : 'UPDATE `users` SET `id` = CASE WHEN `id` = 1 THEN 1 ELSE `id` END, `name` = CASE WHEN `id` = 1 THEN 'Johnson' ELSE `name` END WHERE ( `id` IN (1) )'
};
```

### Update with where
```js
let data = await SQL.query( 'users' ).where( 'id = 1' ).update( { name: 'Johnson' } );
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 1,
	inserted_id   : null,
	inserted_ids  : [],
	changed_id    : null,
	changed_ids   : [],
	row           : null,
	rows          : [],
	sql_time      : 1,
	time          : 1,
	query         : 'UPDATE `users` SET `name` = 'Johnson' WHERE `id` = 1 '
};
```


### Update with string
```js
let data = await SQL.query( 'users' ).where( 'id = 1' ).update( 'name = :?', 'Johnson' } );
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 1,
	inserted_id   : null,
	inserted_ids  : [],
	changed_id    : null,
	changed_ids   : [],
	row           : null,
	rows          : [],
	sql_time      : 1,
	time          : 1,
	query         : 'UPDATE `users` SET `name` = 'Johnson' WHERE `id` = 1 '
};
```

## Insert

### .insert( data[, ignore = false] )

- `data` {Array|Object}
- `ignore` {Boolean}

```js
let data = await SQL.query( 'users' ).insert( { id: 1, name: 'John', surname: 'D.' } );
```

Output  
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 1,
	inserted_id   : 1,
	inserted_ids  : [ 1 ],
	changed_id    : null,
	changed_ids   : [ 1 ],
	row           : null,
	rows          : [],
	sql_time      : 1,
	time          : 1,
	query         : 'INSERT INTO `users` ( id, name, surname ) VALUES ( 1, 'John', 'D.' )'
};
```

```js
let data = await SQL.query( 'users' ).insert( { id: 1, name: 'John', surname: 'D.' }, true );
```

Output  
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 1,
	inserted_id   : 1,
	inserted_ids  : [ 1 ],
	changed_id    : null,
	changed_ids   : [ 1 ],
	row           : null,
	rows          : [],
	sql_time      : 1,
	time          : 1,
	query         : 'INSERT IGNORE INTO `users` ( id, name, surname ) VALUES ( 1, 'John', 'D.' )'
};
```

## Set

### .set( data )

- `data` {Array|Object}

```js
let data = await SQL.query( 'users' ).set( { id: 1, name: 'John', surname: 'D.' } );
```

Output
```
{
	ok            : true,
	error         : null,
	affected_rows : 1,
	changed_rows  : 1,
	inserted_id   : 1,
	inserted_ids  : [ 1 ],
	changed_id    : null,
	changed_ids   : [ 1 ],
	row           : null,
	rows          : [],
	sql_time      : 1,
	time          : 1
};
```
