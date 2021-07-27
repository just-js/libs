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
    // todo allow specifying start and end of rows
    // this is very slow!!
    query.call(err => {
      if (err) return reject(err)
      if (query.raw) {
        resolve(query.getRows())
        return
      }
      const { fields } = query
      const rows = query.getRows().map(r => {
        const row = {}
        let i = 0
        for (const field of fields) {
          row[field.name] = r[i++]
        }
        return row
      })
      resolve(rows)
    })
  })
}

function append (query, syncIt) {
  return new Promise((resolve, reject) => {
    query.append(err => {
      if (err) return reject(err)
      if (query.raw) {
        resolve(query.getRows())
        return
      }
      const { fields } = query
      const rows = query.getRows().map(r => {
        const row = {}
        let i = 0
        for (const field of fields) {
          row[field.name] = r[i++]
        }
        return row
      })
      resolve(rows)
    }, syncIt)
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

function getStatus (sock) {
  const { parser } = sock
  const { query, len } = parser
  const { start } = query
  just.print(`getStatus(${start}, ${len})`)
}

function getRows (connection) {
  const { fields, query, buf, dv, u8 } = connection.sock.parser
  const { start, rows } = query
  let off = start
  const result = []
  let i = 0
  let j = 0
  let len = 0
  //let fieldLen = dv.getUint32(off + 1)
  //off += fieldLen + 1
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
        if (oid === INT4OID) {
          row[name] = parseInt(buf.readString(len, off), 10)
        } else {
          row[name] = buf.readString(len, off)
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
    if (u8[off] === 84) {
      len = dv.getUint32(off + 1)
      off += len
    }
  }
  return result
}

class Query {
  constructor (sql, { formats = [], fields = [], params = [], name = 'adhoc', maxRows = 0, portal = '', htmlEscape = false, raw = false }) {
    this.sql = sql
    this.sock = null
    this.query = null
    this.raw = raw
    this.opts = {
      formats,
      sql,
      fields,
      name,
      portal,
      maxRows,
      params,
      htmlEscape
    }
  }

  async bind (connection) {
    const { sock } = connection
    this.sock = sock
    this.query = await compile(sock, this.opts)
    this.query.raw = this.raw
    this.query.fields = sock.parser.fields.slice(0)
  }

  async call (...args) {
    if (args && args.length) this.query.params = args
    return call(this.query)
  }

  async append (...args) {
    if (args && args.length) this.query.params = args
    return append(this.query, true)
  }

  async appendBuffer (...args) {
    if (args && args.length) this.query.params = args
    return append(this.query, false)
  }

  status () {
    return getStatus(this.sock)
  }

  send () {
    return this.query.send()
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
    const { parser } = this.sock
    const { query, status, errors } = parser
    return { count: query.rows, status: String.fromCharCode(status), errors: errors.slice(0) }
  }

  async close () {
    await terminate(this.sock)
  }
}

// helper functions
const { constants } = pg
const { INT4OID } = constants.fieldTypes

const statSql = `
SELECT 'session' AS name, row_to_json(t) AS data
FROM (SELECT
   (SELECT count(*) FROM pg_stat_activity WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "total",
   (SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND datname = (SELECT datname FROM pg_database WHERE oid = 16385))  AS "active",
   (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle' AND datname = (SELECT datname FROM pg_database WHERE oid = 16385))  AS "idle"
) t
UNION ALL
SELECT 'tps' AS name, row_to_json(t) AS data
FROM (SELECT
   (SELECT sum(xact_commit) + sum(xact_rollback) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "tx",
   (SELECT sum(xact_commit) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "commit",
   (SELECT sum(xact_rollback) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "rollback"
) t
UNION ALL
SELECT 'ti' AS name, row_to_json(t) AS data
FROM (SELECT
   (SELECT sum(tup_inserted) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "insert",
   (SELECT sum(tup_updated) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "update",
   (SELECT sum(tup_deleted) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "delete"
) t
UNION ALL
SELECT 'to' AS name, row_to_json(t) AS data
FROM (SELECT
   (SELECT sum(tup_fetched) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "fetch",
   (SELECT sum(tup_returned) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "return"
) t
UNION ALL
SELECT 'bio' AS name, row_to_json(t) AS data
FROM (SELECT
   (SELECT sum(blks_read) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "read",
   (SELECT sum(blks_hit) FROM pg_stat_database WHERE datname = (SELECT datname FROM pg_database WHERE oid = 16385)) AS "hit"
) t
`

async function compileQuery (db, sql, name, fields = [], formats = [], params = [], htmlEscape = false, raw = false) {
  const opts = { fields, formats, params, name, htmlEscape, raw }
  const query = new Query(sql, opts)
  await query.bind(db)
  return query
}

function getStatSql (tables) {
  return tables.map(name => {
    return `
UNION ALL
SELECT '${name}' AS name, row_to_json(t) AS data
FROM (SELECT
  (SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements WHERE query ~* '[[:<:]]${name}[[:>:]]') AS "call",
  (SELECT COALESCE(SUM(rows), 0) FROM pg_stat_statements WHERE query ~* '[[:<:]]${name}[[:>:]]' AND query ~* 'select') AS "select",
  (SELECT COALESCE(SUM(rows), 0) FROM pg_stat_statements WHERE query ~* '[[:<:]]${name}[[:>:]]' AND query ~* 'update') AS "update"
) t`
  }).join('')
}

function getIds (count) {
  const updates = []
  for (let i = 1; i < (count * 2); i += 2) {
    updates.push(`$${i}`)
  }
  return updates.join(',')
}

function getClauses (count) {
  const clauses = []
  for (let i = 1; i < (count * 2); i += 2) {
    clauses.push(`when $${i} then $${i + 1}`)
  }
  return clauses.join('\n')
}

// todo - handle strings
async function compileBatchUpdate (db, name, table, field, id, updates = 5, flush = true) {
  const opts = {
    formats: [pg.BinaryInt],
    params: Array(updates * 2).fill(0),
    name: `${name}.${updates}`
  }
  const sql = []
  sql.push(`update ${table} set ${field} = CASE ${id}`)
  sql.push(getClauses(updates))
  sql.push(`else ${field}`)
  sql.push(`end where ${id} in (${getIds(updates)})`)
  const query = new Query(sql.join('\n'), opts)
  await query.bind(db, flush)
  return query
}

function queryHandler (query, results, resolve, reject, final = false) {
  return err => {
    if (err) return reject(err)
    if (query.raw) {
      results.push(query.getRows())
      if (final) resolve(results)
      return
    }
    const { fields } = query
    const rows = query.getRows().map(r => {
      const row = {}
      let i = 0
      for (const field of fields) {
        row[field.name] = r[i++]
      }
      return row
    })
    results.push(rows)
    if (final) resolve(results)
  }
}

function compileMultiQuery (q, argFn = () => []) {
  const results = []
  const { query } = q
  return (n = 1) => {
    return new Promise((resolve, reject) => {
      for (let i = 1; i < n; i++) {
        query.params = argFn()
        query.append(queryHandler(query, results, resolve, reject), false)
      }
      query.params = argFn()
      query.append(queryHandler(query, results, resolve, reject, true), true)
      results.length = 0
      query.send()
    })
  }
}

/*
function onMulti () {
  const [id, randomNumber] = getWorldById.getRows()[0]
  results.push({ id, randomNumber })
  if (results.length === queries) {
    const json = JSON.stringify(results)
    sock.writeString(`${rJSON}${json.length}${END}${json}`)
    queries = 0
  }
}

function compileMultiQuery2 (query, argFn = () => []) {
  return (n = 1) => {
    const promises = []
    for (let i = 1; i < n; i++) {
      promises.push(query.appendBuffer(...argFn()))
    }
    promises.push(query.append(...argFn()))
    query.send()
    return Promise.all(promises)
  }
}
*/

async function createConnectionPool (dbConfig, poolSize, onConnect) {
  const clients = []
  for (let i = 0; i < poolSize; i++) {
    const db = new Connection(dbConfig)
    await db.connect()
    await onConnect(db)
    clients.push(db)
  }
  return clients
}

async function getStats (db, tables = []) {
  if (!db.getStats) {
    const sql = `${statSql}${getStatSql(tables)}`
    db.getStats = await compileQuery(db, sql, 'getStats', [], [], [])
  }
  return (await db.getStats.call()).reduce((stats, row) => {
    stats[row.name] = JSON.parse(row.data)
    return stats
  }, {})
}

module.exports = { createConnectionPool, Connection, Query, pg, getRows, compile: compileQuery, getStats, compileBatchUpdate, compileMultiQuery }
