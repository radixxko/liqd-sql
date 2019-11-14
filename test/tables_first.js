module.exports =
{
	primary_string :
	{
		columns :
		{
			id: { type: 'VARCHAR:255' },
			name: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	primary_number :
	{
		columns :
		{
			id: { type: 'BIGINT:20' },
			name: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	selected :
	{
		columns :
		{
			id: { type: 'VARCHAR:255' },
			name: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	selected2 :
	{
		columns :
		{
			id: { type: 'VARCHAR:255' },
			name: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	selected3 :
	{
		columns :
		{
			id: { type: 'BIGINT' },
			name: { type: 'VARCHAR:255' },
			data: { type: 'TEXT' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	table_one :
	{
		columns :
		{
			id: { type: 'VARCHAR:255' },
			name: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	table_two :
	{
		columns :
		{
			id: { type: 'VARCHAR:255' },
			name: { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	truncate :
	{
		columns :
		{
			id       : { type: 'BIGINT:UNSIGNED', increment: true },
			position : { type: 'BIGINT:UNSIGNED' },
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	reserved :
	{
		columns :
		{
			id       : { type: 'BIGINT:UNSIGNED', increment: true },
			group    : { type: 'BIGINT:UNSIGNED' },
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	collate :
	{
		columns :
		{
			id    : { type: 'BIGINT:UNSIGNED', increment: true },
			value : { type: 'VARCHAR:255' },
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
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
	preset :
	{
		columns :
		{
			id   : { type: 'BIGINT:UNSIGNED' },
			name : { type: 'VARCHAR:32' },
			surname : { type: 'VARCHAR:32', default: '' },
			description : { type: 'VARCHAR:255', default: '' },
			age : { type: 'INT:UNSIGNED', default: 'NULL', null: true },
			number : { type: 'INT:UNSIGNED', default: 0 }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
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
	set_numbers :
	{
		columns :
		{
			id          : { type: 'BIGINT:UNSIGNED' },
			uid         : { type: 'BIGINT:UNSIGNED', default: 0 },
			value    	: { type: 'DECIMAL:12,9', default: 0 }
		},
		indexes :
		{
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	set_test :
	{
		columns :
		{
			name : { type: 'VARCHAR:255', default: 'name' },
			uid  : { type: 'BIGINT', default: 'NULL', null: true }
		},
		indexes :
		{
			primary : '',
			unique  : ['name'],
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
	update_users_4 :
	{
		columns :
		{
			id    : { type: 'BIGINT:UNSIGNED' },
			groups : { type: 'BIGINT:UNSIGNED' },
			uid   : { type: 'BIGINT:UNSIGNED' },
			name  : { type: 'VARCHAR:255' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [ 'groups,uid' ],
			index   : []
		}
	},
	users_table :
	{
		columns :
		{
			id    : { type: 'BIGINT:UNSIGNED', increment: true },
			name  : { type: 'VARCHAR:255' }
		},
		indexes : {
			primary : 'id',
			unique  : [],
			index   : []
		}
	},
	set_string :
	{
		columns :
		{
			id : { type: 'BIGINT:UNSIGNED', increment: true },
			uid : { type: 'BIGINT:UNSIGNED' },
			string1: { type: 'VARCHAR:255' },
			string2: { type: 'VARCHAR:255' },
			string3: { type: 'VARCHAR:255' },
			string4: { type: 'VARCHAR:12' },
			string5: { type: 'TEXT' },
			string6: { type: 'TEXT' },
			string7: { type: 'SET:first,second,third' },
			string8: { type: 'ENUM:north,west,south,east' }
		},
		indexes :
		{
			primary : 'id',
			unique  : [ 'uid' ],
			index   : []
		}
	},
	insert_string :
	{
		columns :
		{
			id : { type: 'BIGINT:UNSIGNED', increment: true },
			string1: { type: 'VARCHAR:255' },
			string2: { type: 'VARCHAR:255' },
			string3: { type: 'VARCHAR:255' },
			string4: { type: 'VARCHAR:12' },
			string5: { type: 'TEXT' },
			string6: { type: 'TEXT' },
			string7: { type: 'SET:first,second,third' },
			string8: { type: 'ENUM:north,west,south,east' }
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
	}
};
