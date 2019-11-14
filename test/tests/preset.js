'use strict';

const assert = require('assert');
const SQL = new (require('../../lib/sql.js'))( config );
let tables = require('./../all_tables.js');

it( 'Create', async() =>
{
    await SQL.table( 'preset').drop();
    await SQL.table( tables['preset'], 'preset' ).create();
}).timeout(100000);

it( 'Insert', async() =>
{
    let cnt = 0;
    let insert = await SQL.query( 'preset' ).insert( [ { id: 1, name: 'John' }, { id: 2, name: 'Max' }, { id: 3, name: 'George' }, { id: 4, name: 'Janet' }, { id: 5, name: 'Kate' } ] );
    assert.ok( insert.ok && insert.changed_rows === 5, 'Test insert preset '+(++cnt)+' failed ' + JSON.stringify( insert, null, '  ' ) );
}).timeout(100000);

it( 'Preset', async() =>
{
    let preset, cnt = 0;

    preset = await SQL.query( 'preset').preset( [ { id: 2, surname: 'ata', name: 'Max', age: 20 }, { id: 12345, surname: 'A.', name: 'Sam' }, { id: 1, surname: 'at', name: 'John' }, { id: 12313131, surname: 'A.', name: 'Sam', age: 25 } ] );
    await config.compare_array( preset.rows, [
        { id: 2, surname: 'ata', name: 'Max', age: 20, description: '', number: 0 },
        { id: 12345, surname: 'A.', name: 'Sam', description: '', age: null, number: 0 },
        { id: 1, surname: 'at', name: 'John', description: '', age: null, number: 0 },
        { id: 12313131, surname: 'A.', name: 'Sam', age: 25, description: '', number: 0 } ], ++cnt, preset, 'Preset' );
    preset = await SQL.query( 'preset').preset( [ { id: 2, surname: 'ata', name: 'Max', age: 20 }, { id: 12345, surname: 'A.', name: 'Sam' }, { id: 1, surname: 'at', name: 'John' }, { id: 12313131, surname: 'A.', name: 'Sam', age: 25 } ], [ 'id', 'name', 'surname', 'age' ] );
    await config.compare_array( preset.rows, [
        { id: 2, surname: 'ata', name: 'Max', age: 20 },
        { id: 12345, surname: 'A.', name: 'Sam', age: null },
        { id: 1, surname: 'at', name: 'John', age: null },
        { id: 12313131, surname: 'A.', name: 'Sam', age: 25 } ], ++cnt, preset, 'Preset' );


}).timeout(100000);
