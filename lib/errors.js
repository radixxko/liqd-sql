'use strict';
let err = {
	ER_PARSE_ERROR    : 'EREQUEST',
	ER_NO_SUCH_TABLE  : 'EREQUEST',
	UNDEFINED_TABLE   : 'EREQUEST',
	ECONNREFUSED      : 'ECONNCLOSED',
	ER_BAD_FIELD_ERROR : 'EREQUEST'
};

module.exports = class SQLError
{
	constructor( error )
	{
		if( typeof error === 'string' ){ this.code = ( err.hasOwnProperty( error ) ? err[ error ] : error ); }
		else if( error && error.hasOwnProperty( 'code' ) ){ this.code = ( err.hasOwnProperty( error.code ) ? err[ error.code ] : error.code ); }
		else { this.code = 'UNKNOWN_ERROR_CODE'; }

		this.message = '';
		this.full = error;
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


	static list()
	{
		return [
			{ code: 'EMPTY_QUERY',      message: 'Query is empty', type: 'Connector' },
			{ code: 'UNDEFINED_TABLE',  message: 'Table is undefined', type: 'Connector' },
			{ code: 'INVALID_ENTRY',    message: 'Missing one of the required entry parameters', type: 'Connector' },
			{ code: 'ELOGIN',           message: 'Login failed.', type: 'ConnectionError' },
			{ code: 'ETIMEOUT',         message: 'Connection timeout.', type: 'ConnectionError' },
			{ code: 'EDRIVER',          message: 'Unknown driver.', type: 'ConnectionError' },
			{ code: 'EALREADYCONNECTED',  message: 'Database is already connected!', type: 'ConnectionError' },
			{ code: 'EALREADYCONNECTING', message: 'Already connecting to database!', type: 'ConnectionError' },
			{ code: 'ENOTOPEN',         message: 'Connection not yet open.', type: 'ConnectionError' },
			{ code: 'EINSTLOOKUP',      message: 'Instance lookup failed.', type: 'ConnectionError' },
			{ code: 'ESOCKET',          message: 'Socket error.', type: 'ConnectionError' },
			{ code: 'ECONNCLOSED',      message: 'Connection is closed.', type: 'ConnectionError' },
			{ code: 'ENOTBEGUN',        message: 'Transaction has not begun.', type: 'TransactionError' },
			{ code: 'EALREADYBEGUN',    message: 'Transaction has already begun.', type: 'TransactionError' },
			{ code: 'EREQINPROG',       message: 'Can\'t commit/rollback transaction. There is a request in progress.', type: 'TransactionError' },
			{ code: 'EABORT',           message: 'Transaction has been aborted.', type: 'TransactionError' },
			{ code: 'EREQUEST',         message: 'Message from SQL Server. Error object contains additional details.', type: 'RequestError' },
			{ code: 'ECANCEL',          message: 'Cancelled.', type: 'RequestError' },
			{ code: 'ETIMEOUT',         message: 'Request timeout.', type: 'RequestError' },
			{ code: 'EARGS',            message: 'Invalid number of arguments.', type: 'RequestError' },
			{ code: 'EINJECT',          message: 'SQL injection warning.', type: 'RequestError' },
			{ code: 'ENOCONN',          message: 'No connection is specified for that request.', type: 'RequestError' },
			{ code: 'EARGS',            message: 'Invalid number of arguments.', type: 'PreparedStatementError' },
			{ code: 'EINJECT',          message: 'SQL injection warning.', type: 'PreparedStatementError' },
			{ code: 'EALREADYPREPARED', message: 'Statement is already prepared.', type: 'PreparedStatementError' },
			{ code: 'ENOTPREPARED',     message: 'Statement is not prepared.', type: 'PreparedStatementError' }
		];
	}
}
