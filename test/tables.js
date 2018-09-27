module.exports =
{
	create_user :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED', increment: true },
			name : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	create_user_2 :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED', increment: true },
			name : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	errors_list :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED' },
			name : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	tests :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED', increment: true },
			name : { type: 'VARCHAR:255', default: 'name' },
			uid  : { type: 'BIGINT', default: 'NULL', null: true }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	insert_users :
	{
		columns :
		{
			id          : { type: 'BIGINT:UNSIGNED', increment: true },
			name        : { type: 'VARCHAR:255' },
			description : { type: 'TEXT', null: true },
			created     : { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			surname     : { type: 'VARCHAR:55', null: true }
		},
		indexes :
		{
			primary : 'id',
			unique  : ['name'],
			index   : 'surname'
		}
	},
	insert_users_2 :
	{
		columns :
		{
			id      : { type: 'BIGINT:UNSIGNED' },
			name    : { type: 'VARCHAR:255' },
			surname : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id,name',
			unique  : []
		}
	},
	insert_users_3 :
	{
		columns :
		{
			id      : { type: 'BIGINT:UNSIGNED' },
			name    : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : []
		}
	},
	join_users :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED' },
			name : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	join_address :
	{
		columns :
		{
			id     : { type: 'BIGINT:UNSIGNED' },
			active : { type: 'TINYINT', default: 1 },
			city   : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	set_users :
	{
		columns :
		{
			id          : { type: 'BIGINT:UNSIGNED', increment: true },
			name        : { type: 'VARCHAR:255' },
			description : { type: 'TEXT', null: true },
			created     : { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			surname     : { type: 'VARCHAR:55', null: true }
		},
		indexes :
		{
			primary : 'id',
			unique  : 'name',
			index   : [ 'surname' ]
		}
	},
	set_address :
	{
		columns :
		{
			id          : { type: 'BIGINT:UNSIGNED', increment: true },
			addressID   : { type: 'BIGINT:UNSIGNED' },
			name        : { type: 'VARCHAR:255' },
			description : { type: 'TEXT', null: true },
			created     : { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			city        : { type: 'VARCHAR:55', null: true }
		},
		indexes : {
			primary : 'id',
			unique  : 'addressID,name',
			index   : [ 'city' ]
		}
	},
	set_phones :
	{
		columns :
		{
			userID : { type: 'BIGINT:UNSIGNED' },
			phone  : { type: 'BIGINT:UNSIGNED' }
		},
		indexes : {
			primary : '',
			unique  : [ ],
			index   : [ ]
		}
	},
	set_names :
	{
		columns :
		{
			id          : { type: 'BIGINT:UNSIGNED' },
			name        : { type: 'VARCHAR:255' },
			surname     : { type: 'VARCHAR:55', null: true }
		},
		indexes :
		{
			primary : 'id',
			unique  : 'name',
			index   : []
		}
	},
	table_users :
	{
		columns :
		{
			id      : { type: 'BIGINT:UNSIGNED', increment: true },
			name    : { type: 'VARCHAR:255' },
			surname : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes :
		{
			primary : 'id',
			unique  : 'name',
			index   : [ 'surname' ]
		}
	},
	table_address :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED', increment: true },
			name : { type: 'VARCHAR:255' },
			city : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes :
		{
			primary : null,
			unique  : [ 'name' ],
			index   : [ 'id' ]
		}
	},
	table_cities :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED', increment: true },
			name : { type: 'VARCHAR:255' },
			city : { type: 'VARCHAR:255', null: true, default: 'NULL' }
		},
		indexes :
		{
			primary : null,
			unique  : 'name',
			index   : [ 'id' ]
		}
	},
	test_users :
	{
		columns : {
			id          : { type: 'BIGINT:UNSIGNED', increment: true },
			name        : { type: 'VARCHAR:255', null: true },
			description : { type: 'TEXT', null: true },
			created     : { type: 'TIMESTAMP', default: 'CURRENT_TIMESTAMP', update: 'CURRENT_TIMESTAMP' },
			surname     : { type: 'VARCHAR:55', null: true, default: 'NULL' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : [ 'name' ]
		}
	},
	union_users :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED', increment: true },
			name : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	union_address :
	{
		columns :
		{
			id     : { type: 'BIGINT:UNSIGNED', increment: true },
			street : { type: 'VARCHAR:255' },
			city   : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	update_users :
	{
		columns :
		{
			id    : { type: 'BIGINT:UNSIGNED' },
			uid   : { type: 'BIGINT:UNSIGNED' },
			name  : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [ 'uid' ],
			index   : []
		}
	},
	update_users_2 :
	{
		columns :
		{
			id      : { type: 'BIGINT:UNSIGNED' },
			'u-id'  : { type: 'BIGINT:UNSIGNED' },
			name    : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : '',
			unique  : [],
			index   : []
		}
	},
	update_users_3 :
	{
		columns :
		{
			name    : { type: 'VARCHAR:255' },
			surname : { type: 'VARCHAR:255' },
			city 	: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : '',
			unique  : [ 'name,surname' ],
			index   : []
		}
	}
};
