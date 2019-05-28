'use strict';

const list = {
	ER_PARSE_ERROR     : { type: 'query',           message: 'Parse error' },
	ER_NO_SUCH_TABLE   : { type: 'query',           message: 'Table doesn\'t exist' },
	UNDEFINED_TABLE    : { type: 'query',           message: 'Bad field' },
	ECONNREFUSED       : { type: 'connect',         message: 'Connection refused.' },
	PROTOCOL_CONNECTION_LOST : { type: 'connect', reconnect : true,        message: '' },
	ECONNRESET         : { type: 'connect', reconnect : true,        message: 'Connection refused.' },
	ER_BAD_FIELD_ERROR : { type: 'query',           message: 'Bad field' },
	EMPTY_QUERY        : { type: 'query',           message: 'Query is empty' },
	UNDEFINED_TABLE    : { type: 'query',   message: 'Table is undefined' },
	INVALID_ENTRY      : { type: 'query',   message: 'Missing one of the required entry parameters' },
	ELOGIN             : { type: 'connect',           message: 'Login failed.' },
	EDRIVER            : { type: 'connect',          message: 'Unknown driver.' },
	EALREADYCONNECTED  : { type: 'connect',  message: 'Database is already connected!' },
	EALREADYCONNECTING : { type: 'connect', message: 'Already connecting to database!' },
	ENOTOPEN           : { type: 'connect', reconnect : true,        message: 'Connection not yet open.' },
	EINSTLOOKUP        : { type: 'connect', reconnect : true,     message: 'Instance lookup failed.' },
	ESOCKET            : { type: 'connect', reconnect : true,          message: 'Socket error.' },
	ECONNCLOSED        : { type: 'connect', reconnect : true, message: 'Connection is closed.' },
	ENOTBEGUN          : { type: 'transation', message: 'Transaction has not begun.' },
	EALREADYBEGUN      : { type: 'transation', message: 'Transaction has already begun.' },
	EREQINPROG         : { type: 'transation', message: 'Can\'t commit/rollback transaction. There is a request in progress.' },
	EABORT             : { type: 'transation', message: 'Transaction has been aborted.' },
	EREQUEST           : { type: 'query',                     message: 'Message from SQL Server. Error object contains additional details.' },
	ECANCEL            : { type: 'query', reconnect : true, message: 'Cancelled.' },
	ETIMEOUT           : { type: 'query', reconnect : true, message: 'Request timeout.' },
	EARGS              : { type: 'query',                     message: 'Invalid number of arguments.' },
	EINJECT            : { type: 'query',                     message: 'SQL injection warning.' },
	ENOCONN            : { type: 'query',   reconnect : true, message: 'No connection is specified for that request.' },
	EARGS              : { type: 'prepared_statement', message: 'Invalid number of arguments.' },
	EINJECT            : { type: 'prepared_statement', message: 'SQL injection warning.' },
	EALREADYPREPARED   : { type: 'prepared_statement', message: 'Statement is already prepared.' },
	ENOTPREPARED       : { type: 'prepared_statement', message: 'Statement is not prepared.' },
	ER_ACCESS_DENIED_ERROR : { type: 'connect', message: 'Access denied.' },
	ER_DUP_ENTRY       : { type: 'query', error: 'ER_DUP_ENTRY', message: 'Unique constraint violated' },
	'42P01'            : { type: 'query',           message: 'Table doesn\'t exist' },
	'918'              : { type: 'query',           message: 'Column is ambiguously defined' },
	'42703'            : { type: 'query',           message: 'Undefined column' },
	'942'              : { type: 'query',           message: 'Table doesn\'t exist' },
	'1747'             : { type: 'query',           message: 'Invalid user.table.column, table.column, or columns specification' },
	'923'              : { type: 'query',           message: 'FROM keyword not found where expected' },
	'933'              : { type: 'query',           message: 'SQL command not properly ended' },
	'904'              : { type: 'query',           message: 'Invalid column name or the column name is missing' },
	'12543'            : { type: 'query',   reconnect : true, message: 'No connection is specified for that request.' },
	'12541'            : { type: 'query',   reconnect : true, message: 'No listener' },
	'1'                : { type: 'query', error: 'ER_DUP_ENTRY', message: 'Unique constraint violated' },
};

module.exports = class SQLError
{
	constructor( error )
	{
		let err_code = ( typeof error === 'string' ? error : ( error && error.hasOwnProperty( 'code' ) ? error.code : 'UNKNOWN_ERROR_CODE' ) )

		this.full = ( list.hasOwnProperty( err_code ) ? list[err_code] : err_code );
		this.message = ( this.full && this.full.message ? this.full.message : '' );
		this.type = ( this.full && this.full.type ? this.full.type : '' );
		this.reconnect = ( this.full && this.full.reconnect ? this.full.reconnect : '' );

		//this.full = error;
	}

	get()
	{
		return this;
	}

	//toString()
	//{
	//  return this.code.toString();
	//}

	//isConnectionProblem()
	//{

	//}

	//isFatal()
	//{

	//}


	list( err )
	{
		return list;
	}
}
