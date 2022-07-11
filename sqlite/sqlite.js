const { sqlite } = just.library('sqlite')

const checkpoint = {
  SQLITE_CHECKPOINT_PASSIVE:  0,
  SQLITE_CHECKPOINT_FULL:     1,
  SQLITE_CHECKPOINT_RESTART:  2,
  SQLITE_CHECKPOINT_TRUNCATE: 3
}

const v2 = {
  SQLITE_OPEN_READONLY        : 0x00000001,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_READWRITE       : 0x00000002,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_CREATE          : 0x00000004,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_DELETEONCLOSE   : 0x00000008,  /* VFS only */
  SQLITE_OPEN_EXCLUSIVE       : 0x00000010,  /* VFS only */
  SQLITE_OPEN_AUTOPROXY       : 0x00000020,  /* VFS only */
  SQLITE_OPEN_URI             : 0x00000040,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_MEMORY          : 0x00000080,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_MAIN_DB         : 0x00000100,  /* VFS only */
  SQLITE_OPEN_TEMP_DB         : 0x00000200,  /* VFS only */
  SQLITE_OPEN_TRANSIENT_DB    : 0x00000400,  /* VFS only */
  SQLITE_OPEN_MAIN_JOURNAL    : 0x00000800,  /* VFS only */
  SQLITE_OPEN_TEMP_JOURNAL    : 0x00001000,  /* VFS only */
  SQLITE_OPEN_SUBJOURNAL      : 0x00002000,  /* VFS only */
  SQLITE_OPEN_SUPER_JOURNAL   : 0x00004000,  /* VFS only */
  SQLITE_OPEN_NOMUTEX         : 0x00008000,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_FULLMUTEX       : 0x00010000,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_SHAREDCACHE     : 0x00020000,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_PRIVATECACHE    : 0x00040000,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_WAL             : 0x00080000,  /* VFS only */
  SQLITE_OPEN_NOFOLLOW        : 0x01000000,  /* Ok for sqlite3_open_v2() */
  SQLITE_OPEN_EXRESCODE       : 0x02000000  /* Extended result codes */
}

const constants = {
  SQLITE_OK          : 0, // Successful result
  SQLITE_ERROR       : 1, // Generic error
  SQLITE_INTERNAL    : 2, // Internal logic error in SQLite
  SQLITE_PERM        : 3, // Access permission denied
  SQLITE_ABORT       : 4, // Callback routine requested an abort
  SQLITE_BUSY        : 5, // The database file is locked
  SQLITE_LOCKED      : 6, // A table in the database is locked
  SQLITE_NOMEM       : 7, // A malloc() failed
  SQLITE_READONLY    : 8, // Attempt to write a readonly database
  SQLITE_INTERRUPT   : 9, // Operation terminated by sqlite3_interrupt()
  SQLITE_IOERR      : 10, // Some kind of disk I/O error occurred
  SQLITE_CORRUPT    : 11, // The database disk image is malformed
  SQLITE_NOTFOUND   : 12, // Unknown opcode in sqlite3_file_control()
  SQLITE_FULL       : 13, // Insertion failed because database is full
  SQLITE_CANTOPEN   : 14, // Unable to open the database file
  SQLITE_PROTOCOL   : 15, // Database lock protocol error
  SQLITE_EMPTY      : 16, // Internal use only
  SQLITE_SCHEMA     : 17, // The database schema changed
  SQLITE_TOOBIG     : 18, // String or BLOB exceeds size limit
  SQLITE_CONSTRAINT : 19, // Abort due to constraint violation
  SQLITE_MISMATCH   : 20, // Data type mismatch
  SQLITE_MISUSE     : 21, // Library used incorrectly
  SQLITE_NOLFS      : 22, // Uses OS features not supported on host
  SQLITE_AUTH       : 23, // Authorization denied
  SQLITE_FORMAT     : 24, // Not used
  SQLITE_RANGE      : 25, // 2nd parameter to sqlite3_bind out of range
  SQLITE_NOTADB     : 26, // File opened that is not a database file
  SQLITE_NOTICE     : 27, // Notifications from sqlite3_log()
  SQLITE_WARNING    : 28, // Warnings from sqlite3_log()
  SQLITE_ROW        : 100, // sqlite3_step() has another row ready
  SQLITE_DONE       : 101 // sqlite3_step() has finished executing
}

constants.SQLITE_DESERIALIZE_FREEONCLOSE = 1 /* Call sqlite3_free() on close */
constants.SQLITE_DESERIALIZE_RESIZEABLE = 2 /* Resize using sqlite3_realloc64() */
constants.SQLITE_DESERIALIZE_READONLY = 4 /* Database is read-only */

constants.v2 = v2
constants.checkpoint = checkpoint

constants.SQLITE_SERIALIZE_NOCOPY = 0x001

const fieldTypes = {
  SQLITE_INTEGER    : 1,
  SQLITE_FLOAT      : 2,
  SQLITE_TEXT       : 3,
  SQLITE_BLOB       : 4,
  SQLITE_NULL       : 5,
  SQLITE_INT64      : 6
}
constants.fieldTypes = fieldTypes

function getType (type, i) {
  if (type === fieldTypes.SQLITE_INTEGER) {
    return `sqlite.columnInt(stmt, ${i})`
  } else if (type === fieldTypes.SQLITE_INT64) {
    return `sqlite.columnInt64(stmt, ${i})`
  } else if (type === fieldTypes.SQLITE_FLOAT) {
    return `sqlite.columnDouble(stmt, ${i})`
  } else if (type === fieldTypes.SQLITE_NULL) {
    return `sqlite.columnText(stmt, ${i})`
  } else if (type === fieldTypes.SQLITE_TEXT) {
    return `sqlite.columnText(stmt, ${i})`
  } else if (type === fieldTypes.SQLITE_BLOB) {
    return `sqlite.columnBlob(stmt, ${i})`
  } else {
    return `null`
  }
}

function getDefault (type) {
  if (type === fieldTypes.SQLITE_INTEGER) {
    return '0'
  } else if (type === fieldTypes.SQLITE_INT64) {
    return '0n'
  } else if (type === fieldTypes.SQLITE_FLOAT) {
    return '0.0'
  } else if (type === fieldTypes.SQLITE_NULL) {
    return '\'\''
  } else if (type === fieldTypes.SQLITE_TEXT) {
    return '\'\''
  } else if (type === fieldTypes.SQLITE_BLOB) {
    return 'new ArrayBuffer(0)'
  } else {
    return `null`
  }
}

class Row {
  constructor () {

  }
}

class Query {
  constructor (db, sql, maxRows = 1000, name = sql) {
    this.db = db
    this.sql = sql
    this.stmt = null
    this.params = []
    this.types = []
    this.names = []
    this.count = 0
    this.Row = Row
    this.rows = []
    this.maxRows = maxRows
    this.name = name
  }

  prepare (fields = [], params = []) {
    if (fields.length) {
      this.types.length = 0
      this.names.length = 0
    }
    for (const field of fields) {
      const { name, type } = field
      this.types.push(type)
      this.names.push(name)
    }
    this.params = params
    const { db } = this.db
    this.stmt = sqlite.prepare(db, this.sql)
    if (!this.stmt) throw new Error(sqlite.error(db))
    return this
  }

  close () {
    sqlite.finalize(this.stmt)
  }

  compile (maxRows = this.maxRows) {
    const { types, names, params } = this
    this.sqlite = sqlite
    this.constants = constants
    const source = []
    const fParams = []
    source.push(`const { db, stmt, rows, sqlite, constants } = this`)
    source.push(`  const { SQLITE_OK, SQLITE_ROW, SQLITE_DONE } = constants`)
    source.push(`  const { error, bindText, bindInt, bindDouble, bindInt64, bindBlob, step, reset } = sqlite\n`)
    let i = 0
    for (const param of params) {
      if (param.type) {
        fParams.push(param.name)
        const { name, type } = param
        if (type === fieldTypes.SQLITE_TEXT) {
          source.push(`  if (bindText(stmt, ${i + 1}, ${name}) !== SQLITE_OK) throw new Error(error(db))`)
        } else if (type === fieldTypes.SQLITE_INTEGER) {
          source.push(`  if (bindInt(stmt, ${i + 1}, Number(${name})) !== SQLITE_OK) throw new Error(error(db))`)
        } else if (type === fieldTypes.SQLITE_FLOAT) {
          source.push(`  if (bindDouble(stmt, ${i + 1}, Number(${name})) !== SQLITE_OK) throw new Error(error(db))`)
        } else if (type === fieldTypes.SQLITE_INT64) {
          source.push(`  if (bindInt64(stmt, ${i + 1}, BigInt(${name})) !== SQLITE_OK) throw new Error(error(db))`)
        } else if (type === fieldTypes.SQLITE_BLOB) {
          source.push(`  if (bindBlob(stmt, ${i + 1}, ${name}) !== SQLITE_OK) throw new Error(error(db))`)
        }
        i++
        continue
      }
      if (param === fieldTypes.SQLITE_TEXT) {
        source.push(`  if (bindText(stmt, ${i + 1}, arguments[${i}]) !== SQLITE_OK) throw new Error(error(db))`)
      } else if (param === fieldTypes.SQLITE_INTEGER) {
        source.push(`  if (bindInt(stmt, ${i + 1}, Number(arguments[${i}])) !== SQLITE_OK) throw new Error(error(db))`)
      } else if (param === fieldTypes.SQLITE_FLOAT) {
        source.push(`  if (bindDouble(stmt, ${i + 1}, Number(arguments[${i}])) !== SQLITE_OK) throw new Error(error(db))`)
      } else if (param === fieldTypes.SQLITE_INT64) {
        source.push(`  if (bindInt64(stmt, ${i + 1}, BigInt(arguments[${i}])) !== SQLITE_OK) throw new Error(error(db))`)
      } else if (param === fieldTypes.SQLITE_BLOB) {
        source.push(`  if (bindBlob(stmt, ${i + 1}, arguments[${i}]) !== SQLITE_OK) throw new Error(error(db))`)
      }
      i++
    }
    // TODO: this will break if we exceed maxRows
    source.push(`
  let count = 0
  let ok = step(stmt)
  while (ok === SQLITE_ROW) {`)
    source.push(`    if (count === ${maxRows}) break`)
    source.push('    const row = rows[count]')
    for (let i = 0; i < types.length; i++) {
      source.push(`    row.${names[i]} = ${getType(types[i], i)}`)
    }
    source.push(`    ok = step(stmt)
    count++
  }
  if (ok !== SQLITE_OK && ok !== SQLITE_DONE) {
    throw new Error(error(db))
  }
  this.count = count
  reset(stmt)
  return rows`)
    const text = `  ${source.join('\n').trim()}`
    this.exec = just.vm.compile(text, this.name, fParams, [])
    source.length = 0
    // TODO: use contructor of row so we can do new Row(...)
    const args = names.map((n, i) => `${n} = ${getDefault(types[i])}`).join(', ')
    source.push(`
class Row {
  constructor (${args}) {`)
    i = 0
    for (const name of names) {
      source.push(`    this.${name} = ${name}`)
    }
    source.push(`  }
}
return Row
    `)
    const Row = (just.vm.compile(source.join('\n'), `${this.name}.Row`, [], []))()
    this.Row = Row
    this.rows = new Array(maxRows).fill(0).map(v => new Row())
    return this
  }

  exec (...values) {
    const { params, stmt, types, names, rows, maxRows } = this
    const { db } = this.db

    let i = 0
    // TODO: have a sqlite.binParams, where i can pass in all params as an array and bind them with one c++ call
    // e.g. sqlite.bindParams(stmt, params)
    for (const param of params) {
      const p = param.type ? param.type : param
      if (p === fieldTypes.SQLITE_TEXT) {
        if (sqlite.bindText(stmt, i + 1, values[i]) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (p === fieldTypes.SQLITE_INTEGER) {
        if (sqlite.bindInt(stmt, i + 1, Number(values[i])) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (p === fieldTypes.SQLITE_FLOAT) {
        if (sqlite.bindDouble(stmt, i + 1, Number(values[i])) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (p === fieldTypes.SQLITE_INT64) {
        if (sqlite.bindInt64(stmt, i + 1, BigInt(values[i])) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (p === fieldTypes.SQLITE_BLOB) {
        if (sqlite.bindBlob(stmt, i + 1, values[i]) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      }
      i++
    }
    rows.length = 0
    let count = 0
    let ok = sqlite.step(stmt)
    if (!types.length) {
      const columns = sqlite.columnCount(stmt)
      for (let i = 0; i < columns; i++) {
        const type = sqlite.columnType(stmt, i)
        types.push(type)
        const name = sqlite.columnName(stmt, i)
        names.push(name)
      }
    }
    while (ok === constants.SQLITE_ROW) {
      if (count === maxRows) break
      const row = {}
      // TODO: also here, have a sqlite.readRow() so we can just use one C++ call per row
      // also, a readRows() call would be nice too to read all rows into an array
      // e.g. sqlite.readRow(stmt, types, names)
      // sqlite.readRows(stmt, types, names)
      // even better again, bind these types to handle in C++ with a struct containing the types/names and we only have to pass in the statement
      for (let i = 0; i < types.length; i++) {
        if (types[i] === fieldTypes.SQLITE_INTEGER) {
          row[names[i]] = sqlite.columnInt(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_INT64) {
          row[names[i]] = sqlite.columnInt64(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_FLOAT) {
          row[names[i]] = sqlite.columnDouble(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_NULL) {
          row[names[i]] = sqlite.columnText(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_TEXT) {
          row[names[i]] = sqlite.columnText(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_BLOB) {
          row[names[i]] = sqlite.columnBlob(stmt, i)
        } else {
          row[names[i]] = null
        }
      }
      count++
      rows.push(row)
      ok = sqlite.step(stmt)
    }
    if (ok !== constants.SQLITE_DONE) {
      just.error(`bad query status ${ok}`)
      //just.error(sqlite.error(db))
    }
    if (ok !== constants.SQLITE_OK && ok !== constants.SQLITE_DONE) {
      throw new Error(sqlite.error(db))
    }
    this.count = count
    sqlite.reset(stmt)
    return rows
  }
}

class Database {
  constructor (name = ':memory:') {
    this.name = name
    this.db = null
  }

  releaseMemory () {
    if (!this.db) return
    return sqlite.releaseDBMemory(this.db)
  }

  errorCode () {
    if (!this.db) return
    return sqlite.errCode(this.db)
  }

  errorMessage () {
    if (!this.db) return
    return sqlite.errMessage(this.db)
  }

  open (flags = defaultFlags, vfs) {
    let db
    if (flags) {
      if (vfs) {
        db = sqlite.open(this.name || ':memory:', flags, vfs)
      } else {
        db = sqlite.open(this.name || ':memory:', flags)
      }
    } else {
      db = sqlite.open(this.name || ':memory:')
    }
    if (!db) throw new Error('Failed to open database')
    this.db = db
    return this
  }

  query (sql, name = sql, maxRows = 1000) {
    return new Query(this, sql, maxRows, name)
  }

  checkpoint (mode = checkpoint.SQLITE_CHECKPOINT_FULL) {
    return sqlite.checkpoint(this.db, mode)
  }

  changes () {
    return sqlite.changes(this.db)
  }

  onWal (callback = () => {}) {
    return sqlite.walHook(this.db, callback)
  }

  deserialize (buf, flags, name, size = buf.byteLength) {
    return sqlite.deserialize(this.db, buf, flags, name, size)
  }

  serialize (name, flags = 0) {
    return sqlite.serialize(this.db, name, flags)
  }

  exec (sql, fields = []) {
    const query = new Query(this, sql).prepare(fields)
    const rows = query.exec()
    query.close()
    return rows
  }

  schema () {
    const tables = this.exec('SELECT * FROM sqlite_schema', [
      { name: 'type', type: fieldTypes.SQLITE_TEXT },
      { name: 'name', type: fieldTypes.SQLITE_TEXT },
      { name: 'tableName', type: fieldTypes.SQLITE_TEXT },
      { name: 'rootPage', type: fieldTypes.SQLITE_INT64 },
      { name: 'sql', type: fieldTypes.SQLITE_TEXT },
    ])
    for (const table of tables) {
      table.cols = this.exec(`PRAGMA table_info(${table.name})`, [
        { name: 'cid', type: fieldTypes.SQLITE_INTEGER },
        { name: 'name', type: fieldTypes.SQLITE_TEXT },
        { name: 'type', type: fieldTypes.SQLITE_TEXT },
        { name: 'notnull', type: fieldTypes.SQLITE_INTEGER },
        { name: 'default', type: fieldTypes.SQLITE_INTEGER },
        { name: 'pk', type: fieldTypes.SQLITE_INTEGER }
      ])
    }
    return tables
  }

  close () {
    sqlite.close(this.db)
  }
}

function initialize () {
  return sqlite.initialize()
}

function shutdown () {
  return sqlite.shutdown()
}

function registerVFS (vfs) {
  return sqlite.registerVFS(vfs)
}

function unregisterVFS (vfs) {
  return sqlite.unregisterVFS(vfs)
}

function findVFS (vfs) {
  return sqlite.findVFS(vfs)
}

function memoryUsed () {
  return sqlite.memoryUsed()
}

function memoryHighwater () {
  return sqlite.memoryHighwater()
}

function releaseMemory () {
  return sqlite.releaseMemory()
}

const SQLTypes = {
  int: name => ({ name, type: fieldTypes.SQLITE_INTEGER }),
  int64: name => ({ name, type: fieldTypes.SQLITE_INT64 }),
  text: name => ({ name, type: fieldTypes.SQLITE_TEXT }),
  blob: name => ({ name, type: fieldTypes.SQLITE_BLOB }),
  float: name => ({ name, type: fieldTypes.SQLITE_NULL }),
  null: name => ({ name, type: fieldTypes.SQLITE_FLOAT })
}

const defaultFlags = v2.SQLITE_OPEN_READWRITE | v2.SQLITE_OPEN_SHAREDCACHE | 
  v2.SQLITE_OPEN_NOMUTEX | v2.SQLITE_OPEN_CREATE

module.exports = {
  constants,
  Database,
  sqlite,
  initialize,
  shutdown,
  registerVFS,
  unregisterVFS,
  findVFS,
  memoryUsed,
  memoryHighwater,
  releaseMemory,
  SQLTypes,
  Query,
  defaultFlags
}
