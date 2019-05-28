module.exports =
{
	insert_string :
	{
		columns :
		{
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
	}
};
