const pg = require('transport.js')

function connect (config) {
  return new Promise((resolve, reject) => {
    pg.connect(config, (err, sock) => {
      if (err) return reject(err)
      resolve(sock)
    })
  })
}

function start (sock) {
  return new Promise((resolve, reject) => {
    sock.start(err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function authenticate (sock) {
  return new Promise((resolve, reject) => {
    sock.authenticate(err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function compile (sock, query) {
  return new Promise((resolve, reject) => {
    const result = sock.compile(query, err => {
      if (err) return reject(err)
      resolve(result)
    })
  })
}

function call (query) {
  return new Promise((resolve, reject) => {
    query.call(err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function close (query) {
  return new Promise((resolve, reject) => {
    query.close(true, err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function terminate (sock) {
  return new Promise(resolve => sock.terminate(resolve))
}

function execute (sock, sql) {
  return new Promise((resolve, reject) => {
    sock.execQuery(sql, err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function getRows (connection, off) {
  const { fields, query, buf, dv } = connection.sock.parser
  const { start, rows } = query
  off = off || start
  const result = []
  let i = 0
  let j = 0
  let len = 0
  let fieldLen = dv.getUint32(off + 1)
  off += fieldLen + 1
  let row
  for (i = 0; i < rows; i++) {
    off += 5
    const cols = dv.getUint16(off)
    off += 2
    row = {}
    result.push(row)
    for (j = 0; j < cols; j++) {
      const { name } = fields[j]
      len = dv.getUint32(off)
      const { oid, format } = (fields[j] || fields[0])
      off += 4
      if (format === 0) { // Non-Binary
        if (oid === constants.fieldTypes.INT4OID) {
          row[name] = parseInt(buf.readString(len, off), 10)
        } else {
          row[name] = readString(buf, len, off)
        }
      } else {
        if (oid === INT4OID) {
          row[name] = dv.getInt32(off)
        } else {
          row[name] = buf.slice(off, off + len)
        }
      }
      off += len
    }
  }
  return result
}

class Query {
  constructor (sql, { formats = [], fields = [], params = [], name = 'adhoc', maxRows = 0, portal = '' }) {
    this.sql = sql
    this.sock = null
    this.query = null
    this.opts = {
      formats,
      sql,
      fields,
      name,
      portal,
      maxRows,
      params
    }
  }

  async bind (connection) {
    const { sock } = connection
    this.sock = sock
    this.query = await compile(sock, this.opts)
    this.query.fields = sock.parser.fields.slice(0)
  }

  async call (...args) {
    if (args && args.length) this.query.params = args
    await call(this.query)
    const { fields } = this.query
    const rows = this.query.getRows().map(r => {
      const row = {}
      let i = 0
      for (const field of fields) {
        row[field.name] = r[i++]
      }
      return row
    })
    return rows
  }

  async close () {
    await close(this.query)
  }
}

class Connection {
  constructor (db) {
    this.sock = null
    this.db = db
  }

  async connect () {
    const connection = this
    connection.sock = await connect(connection.db)
    await start(connection.sock)
    await authenticate(connection.sock)
  }

  async query (sql, opts = {}) {
    const query = new Query(sql, opts)
    await query.bind(this)
    return query
  }

  async exec (sql, queries = 1) {
    await execute(this.sock, sql, queries)
    return getRows(this)
  }

  status () {
    const { query, status, errors } = this.sock.parser
    return { count: query.rows, status: String.fromCharCode(status), errors: errors.slice(0) }
  }

  async close () {
    await terminate(this.sock)
  }
}

module.exports = { Connection, Query, pg, getRows }
