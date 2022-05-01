const { sqlite } = just.library('sqlite')

const checkpoint = {
  SQLITE_CHECKPOINT_PASSIVE:  0,
  SQLITE_CHECKPOINT_FULL:     1,
  SQLITE_CHECKPOINT_RESTART:  2,
  SQLITE_CHECKPOINT_TRUNCATE: 3
}

const v2 = {
  SQLITE_OPEN_READONLY:         0x00000001,
  SQLITE_OPEN_READWRITE:        0x00000002,
  SQLITE_OPEN_CREATE:           0x00000004,
  SQLITE_OPEN_URI:              0x00000040,
  SQLITE_OPEN_MEMORY:           0x00000080,
  SQLITE_OPEN_NOMUTEX:          0x00008000,
  SQLITE_OPEN_FULLMUTEX:        0x00010000,
  SQLITE_OPEN_SHAREDCACHE:      0x00020000,
  SQLITE_OPEN_PRIVATECACHE:     0x00040000,
  SQLITE_OPEN_NOFOLLOW:         0x01000000
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

constants.v2 = v2
constants.checkpoint = checkpoint

const fieldTypes = {
  SQLITE_INTEGER    : 1,
  SQLITE_FLOAT      : 2,
  SQLITE_TEXT       : 3,
  SQLITE_BLOB       : 4,
  SQLITE_NULL       : 5
}

class Query {
  constructor (db, sql) {
    this.db = db
    this.sql = sql
    this.stmt = null
    this.fields = []
  }

  prepare (fields = []) {
    this.fields = fields
    const { db } = this.db
    this.stmt = sqlite.prepare(db, this.sql)
    if (!this.stmt) throw new Error(sqlite.error(db))
    return this
  }

  close () {
    sqlite.finalize(this.stmt)
  }

  exec (...values) {
    const { fields, stmt } = this
    const { db } = this.db
    let i = 0
    for (const field of fields) {
      if (field === 'text') {
        if (sqlite.bindText(stmt, i + 1, values[i]) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (field === 'int') {
        if (sqlite.bindInt(stmt, i + 1, Number(values[i])) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (field === 'double') {
        if (sqlite.bindDouble(stmt, i + 1, Number(values[i])) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      } else if (field === 'int64') {
        if (sqlite.bindInt64(stmt, i + 1, BigInt(values[i])) !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
      }
      i++
    }
    const rows = []

    const columns = sqlite.columnCount(stmt)
    const types = []
    const names = []
    for (let i = 0; i < columns; i++) {
      const type = sqlite.columnType(stmt, i)
      types.push(type)
      const name = sqlite.columnName(stmt, i)
      names.push(name)
    }

    let ok = sqlite.step(stmt)

    while (ok === constants.SQLITE_ROW) {
      const row = {}
      for (let i = 0; i < types.length; i++) {
        if (types[i] === fieldTypes.SQLITE_INTEGER) {
          row[names[i]] = parseInt(sqlite.columnText(stmt, i), 10)
        } else if (types[i] === fieldTypes.SQLITE_FLOAT) {
          row[names[i]] = parseFloat(sqlite.columnText(stmt, i))
        } else if (types[i] === fieldTypes.SQLITE_NULL) {
          row[names[i]] = sqlite.columnText(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_TEXT) {
          row[names[i]] = sqlite.columnText(stmt, i)
        } else {
          row[names[i]] = null
        }
      }
      rows.push(row)
      ok = sqlite.step(stmt)
    }
    if (ok !== constants.SQLITE_DONE) {
      just.error(sqlite.error(db))
    }
    if (ok !== constants.SQLITE_OK && ok !== constants.SQLITE_DONE) {
      throw new Error(sqlite.error(db))
    }
    sqlite.reset(stmt)
    return rows
  }
}

class Database {
  constructor (name = ':memory:') {
    this.name = name
    this.db = null
  }

  open (flags, vfs) {
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
    if (!db) throw new Error(sqlite.error(db))
    this.db = db
    return this
  }

  query (sql) {
    return new Query(this, sql)
  }

  execAsync (sql, callback = () => {}) {
    const { db } = this
    const ok = sqlite.exec(db, sql)
  }

  checkpoint (mode) {
    return sqlite.checkpoint(this.db, mode)
  }

  exec (sql) {
    const { db } = this
    const stmt = sqlite.prepare(db, sql)
    if (!stmt) {
      just.error(sqlite.error(db))
      return
    }
    const columns = sqlite.columnCount(stmt)
    const types = []
    const names = []
    for (let i = 0; i < columns; i++) {
      const type = sqlite.columnType(stmt, i)
      types.push(type)
      const name = sqlite.columnName(stmt, i)
      names.push(name)
    }
    const rows = []
    let ok = sqlite.step(stmt)
    while (ok === constants.SQLITE_ROW) {
      const row = {}
      for (let i = 0; i < types.length; i++) {
        if (types[i] === fieldTypes.SQLITE_INTEGER) {
          row[names[i]] = parseInt(sqlite.columnText(stmt, i), 10)
        } else if (types[i] === fieldTypes.SQLITE_FLOAT) {
          row[names[i]] = parseFloat(sqlite.columnText(stmt, i))
        } else if (types[i] === fieldTypes.SQLITE_NULL) {
          row[names[i]] = sqlite.columnText(stmt, i)
        } else if (types[i] === fieldTypes.SQLITE_TEXT) {
          row[names[i]] = sqlite.columnText(stmt, i)
        } else {
          row[names[i]] = null
        }
      }
      rows.push(row)
      ok = sqlite.step(stmt)
    }
    if (ok !== constants.SQLITE_DONE) {
      just.error(sqlite.error(db))
    }
    ok = sqlite.finalize(stmt)
    if (ok !== constants.SQLITE_OK) {
      just.error(sqlite.error(db))
    }
    return rows
  }

  schema () {
    const { db } = this
    const stmt = sqlite.prepare(db, 'SELECT * FROM sqlite_schema')
    if (!stmt) throw new Error(sqlite.error(db))
    let ok = sqlite.step(stmt)
    const tables = []
    while (ok === constants.SQLITE_ROW) {
      const table = {}
      table.columns = []
      table.type = sqlite.columnText(stmt, 0)
      table.name = sqlite.columnText(stmt, 1)
      table.tblName = sqlite.columnText(stmt, 2)
      table.rootPage = parseInt(sqlite.columnText(stmt, 3), 10)
      table.sql = sqlite.columnText(stmt, 4)
      const stmt2 = sqlite.prepare(db, `PRAGMA table_info(${table.name})`)
      if (!stmt2) throw new Error(sqlite.error(db))
      ok = sqlite.step(stmt2)
      while (ok === constants.SQLITE_ROW) {
        const column = {}
        column.cid = parseInt(sqlite.columnText(stmt2, 0), 10)
        column.name = sqlite.columnText(stmt2, 1)
        column.type = sqlite.columnText(stmt2, 2)
        column.notnull = parseInt(sqlite.columnText(stmt2, 3), 10)
        column.dfltValue = parseInt(sqlite.columnText(stmt2, 4) || '0', 10)
        column.pk = parseInt(sqlite.columnText(stmt2, 5), 10)
        table.columns.push(column)
        ok = sqlite.step(stmt2)
      }
      tables.push(table)
      ok = sqlite.step(stmt)
    }
    if (ok !== constants.SQLITE_DONE) throw new Error(sqlite.error(db))
    ok = sqlite.finalize(stmt)
    if (ok !== constants.SQLITE_OK) throw new Error(sqlite.error(db))
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

module.exports = {
  constants,
  Database,
  sqlite,
  initialize,
  shutdown,
  registerVFS,
  unregisterVFS,
  findVFS
}
