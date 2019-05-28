module.exports =
{
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
