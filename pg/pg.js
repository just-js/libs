const { createClient } = require('@tcp')
const { lookup } = require('@dns')
const md5 = require('@md5')
const { html } = just.library('html')
const common = require('common.js')
const parser = require('parser.js')
const { constants } = common
const { createParser } = parser

const { INT4OID, VARCHAROID } = constants.fieldTypes
const {
  AuthenticationOk,
  ErrorResponse,
  RowDescription,
  CommandComplete,
  ParseComplete,
  NoData,
  ReadyForQuery,
  CloseComplete,
  BackendKeyData,
  BindComplete
} = constants.messageTypes

class Messaging {
  constructor (buffer, config) {
    const { sql, formats, portal, name, params, fields, maxRows = 100 } = config
    this.buffer = buffer
    this.view = new DataView(buffer)
    this.bytes = new Uint8Array(buffer)
    this.off = 0
    this.sql = sql
    this.formats = formats
    this.portal = portal
    this.name = name
    this.params = params
    this.fields = fields
    this.maxRows = maxRows
    this.index = 0
  }

  createPrepareMessage (off = this.off) {
    const { view, buffer, formats, name, sql } = this
    const offsets = { start: 0, end: 0 }
    offsets.start = off
    const len = 9 + sql.length + name.length + (formats.length * 4)
    view.setUint8(off++, 80) // 'P'
    view.setUint32(off, len - 1)
    off += 4
    off += buffer.writeString(name, off)
    view.setUint8(off++, 0)
    off += buffer.writeString(sql, off)
    view.setUint8(off++, 0)
    view.setUint16(off, formats.length)
    off += 2
    for (let i = 0; i < formats.length; i++) {
      view.setUint32(off, formats[i].format.oid)
      off += 4
    }
    offsets.len = off - offsets.start
    return { off, offsets, len }
  }

  createDescribeMessage (off = this.off) {
    const { view, buffer, name } = this
    const offsets = { start: 0, end: 0 }
    offsets.start = off
    const len = 7 + name.length
    view.setUint8(off++, 68) // 'D'
    view.setUint32(off, len - 1)
    off += 4
    view.setUint8(off++, 83) // 'S'
    off += buffer.writeString(name, off)
    view.setUint8(off++, 0)
    offsets.len = off - offsets.start
    return { offsets, off, len }
  }

  createFlushMessage (off = this.off) {
    const { view } = this
    const offsets = { start: 0, end: 0 }
    offsets.start = off
    view.setUint8(off++, 72) // 'H'
    view.setUint32(off, 4)
    off += 4
    offsets.len = off - offsets.start
    return { off, offsets }
  }

  createSyncMessage (off = this.off) {
    const { view } = this
    const offsets = { start: 0, end: 0 }
    offsets.start = off
    view.setUint8(off++, 83) // 'S'
    view.setUint32(off, 4)
    off += 4
    offsets.len = off - offsets.start
    return { off, offsets }
  }

  createExecMessage (off = this.off) {
    const { view, buffer, portal, maxRows } = this
    const offsets = { start: 0, end: 0 }
    offsets.start = off
    const len = 6 + portal.length + 4
    view.setUint8(off++, 69) // 'E'
    view.setUint32(off, len - 1)
    off += 4
    if (portal.length) {
      off += buffer.writeString(portal, off)
    }
    view.setUint8(off++, 0)
    view.setUint32(off, maxRows)
    off += 4
    offsets.len = off - offsets.start
    return { len, off, offsets }
  }

  createBindMessage (off = this.off) {
    const { view, buffer, formats, portal, name, params, fields } = this
    const offsets = { start: 0, end: 0 }
    offsets.start = off
    view.setUint8(off++, 66) // 'B'
    off += 4 // length - will be filled in later
    if (portal.length) {
      off += buffer.writeString(portal, off)
      view.setUint8(off++, 0)
      off += buffer.writeString(name, off)
      view.setUint8(off++, 0)
    } else {
      view.setUint8(off++, 0)
      off += buffer.writeString(name, off)
      view.setUint8(off++, 0)
    }
    view.setUint16(off, formats.length || 0)
    off += 2
    for (let i = 0; i < formats.length; i++) {
      view.setUint16(off, formats[i].format)
      off += 2
    }
    view.setUint16(off, params.length || 0)
    off += 2
    const paramStart = off
    for (let i = 0; i < params.length; i++) {
      if ((formats[i] || formats[0]).format === 1) {
        view.setUint32(off, 4)
        off += 4
        view.setUint32(off, params[i])
        off += 4
      } else {
        view.setUint32(off, params[i].length)
        off += 4
        off += buffer.writeString(params[i], off)
      }
    }
    if (fields.length > 0) {
      const format = fields[0].format.format
      let same = true
      for (let i = 1; i < fields.length; i++) {
        if (fields[i].format.format !== format) {
          same = false
          break
        }
      }
      if (same) {
        view.setUint16(off, 1)
        off += 2
        view.setUint16(off, fields[0].format.format)
        off += 2
      } else {
        view.setUint16(off, fields.length)
        off += 2
        for (let i = 0; i < fields.length; i++) {
          view.setUint16(off, fields[i].format.format)
          off += 2
        }
      }
    } else {
      view.setUint16(off, 0)
      off += 2
    }
    offsets.len = off - offsets.start
    view.setUint32(offsets.start + 1, offsets.len - 1)
    return { off, offsets, paramStart }
  }
}

class Query {
  constructor (sock, query, size) {
    this.sock = sock
    this.query = query
    this.size = size
    this.bindings = []
    this.params = []
    this.exec = null
    this.prepare = null
    this.pending = 0
    this.syncing = 0
    this.htmlEscape = html.escape
    this.synced = 0
    this.last = null
  }

  setup () {
    const { size, query, bindings } = this
    bindings.length = 0
    // todo: we should be able to calculate the exact required size
    const flushLen = 5
    const prepareLen = 9 + query.sql.length + query.name.length + (query.formats.length * 4)
    const describeLen = 7 + query.name.length
    const bindLen = 1 + 4 + query.portal.length + 1 + query.name.length + 1 + (query.formats.length * 2) + 2 + (query.params.length * 8) + 2 + (query.fields.length * 2) + 2
    // todo: we need to know size of variable length params
    const execLen = 6 + query.portal.length + 4
    const syncLen = 5
    const bindExecLen = syncLen + (size * (bindLen + execLen))
    const prepareDescribeLen = prepareLen + describeLen + flushLen
    const e = new Messaging(new ArrayBuffer(bindExecLen), query)
    for (let i = 0; i < size; i++) {
      const bind = e.createBindMessage()
      bindings.push(bind)
      e.off = bind.off
      const exec = e.createExecMessage()
      e.off = exec.off
      bind.eoff = exec.off
    }
    this.last = bindings[size - 1]
    e.off = e.createSyncMessage().off

    const p = new Messaging(new ArrayBuffer(prepareDescribeLen), query)
    p.off = p.createPrepareMessage().off
    p.off = p.createDescribeMessage().off
    p.off = p.createFlushMessage().off

    const s = new Messaging(new ArrayBuffer(syncLen), query)
    const sync = s.createSyncMessage()
    s.off = sync.off

    const f = new Messaging(new ArrayBuffer(flushLen), query)
    const flush = f.createSyncMessage()
    f.off = flush.off

    this.exec = e
    this.flush = f
    this.prepare = p
    this.sync = s

    return this
  }

  // prepare the sql statement on the server
  create () {
    const batch = this
    const { sock, prepare } = this
    return new Promise((resolve, reject) => {
      sock.callbacks.push(() => {})
      sock.callbacks.push(() => resolve(batch))
      const r = sock.write(prepare.buffer, prepare.off, 0)
      if (r <= 0) reject(new just.SystemError('Could Not Prepare Queries'))
      // todo: eagain
    })
  }

  compile () {
    const { query, source } = this
    const { read, writeSingle, writeBatch } = source
    // todo. needs to be compiled in a separate context
    if (read.length) this.read = just.vm.compile(read, `${query.name}r.js`, [], [])
    if (writeBatch.length) this.writeBatch = just.vm.compile(writeBatch, `${query.name}w.js`, ['batchArgs', 'first'], [])
    if (writeSingle.length) this.writeSingle = just.vm.compile(writeSingle, `${query.name}s.js`, ['off'], [])
    return this
  }

  generate () {
    const { query } = this
    const { fields, params, formats } = query

    const source = []

    source.push('  const { sock, htmlEscape } = this')
    source.push('  const { state, dv, buf, u8 } = sock.parser')
    source.push('  const { start, rows } = state')
    source.push('  let off = start + 7')
    source.push('  let len = 0')
    source.push('  if (rows === 1) {')
    for (const field of fields) {
      const { name, format, htmlEscape } = field
      if (format.oid === INT4OID) {
        if (format.format === constants.formats.Binary) {
          source.push(`    const ${name} = dv.getInt32(off + 4)`)
          source.push('    off += 8')
        } else {
          source.push('    len = dv.getUint32(off)')
          source.push('    off += 4')
          source.push(`    const ${name} = parseInt(buf.readString(len, off), 10)`)
          source.push('    off += len')
        }
      } else if (format.oid === VARCHAROID) {
        source.push('    len = dv.getUint32(off)')
        source.push('    off += 4')
        if (format.format === constants.formats.Binary) {
          source.push(`    const ${name} = buf.slice(off, off + len)`)
        } else {
          if (htmlEscape) {
            source.push(`    const ${name} = htmlEscape(buf, len, off)`)
          } else {
            source.push(`    const ${name} = buf.readString(len, off)`)
          }
        }
        source.push('    off += len')
      }
    }
    source.push(`    return { ${fields.map(f => f.name).join(', ')} }`)
    source.push('  }')
    source.push('  const result = []')
    source.push('  off = start + 7')
    source.push('  for (let i = 0; i < rows; i++) {')
    for (const field of fields) {
      const { name, format, htmlEscape } = field
      if (format.oid === INT4OID) {
        if (format.format === constants.formats.Binary) {
          source.push(`    const ${name} = dv.getInt32(off + 4)`)
          source.push('    off += 8')
        } else {
          source.push('    len = dv.getInt32(off)')
          source.push('    off += 4')
          source.push(`    const ${name} = parseInt(buf.readString(len, off), 10)`)
          source.push('    off += len')
        }
      } else if (format.oid === VARCHAROID) {
        source.push('    len = dv.getInt32(off)')
        source.push('    off += 4')
        if (format.format === constants.formats.Binary) {
          source.push(`    const ${name} = buf.slice(len, off)`)
        } else {
          if (htmlEscape) {
            source.push(`    const ${name} = htmlEscape(buf, len, off)`)
          } else {
            source.push(`    const ${name} = buf.readString(len, off)`)
          }
        }
        source.push('    off += len')
      }
    }
    source.push('    if (u8[off] === 84) {')
    source.push('      len = dv.getUint32(off + 1)')
    source.push('      off += len')
    source.push('    }')
    source.push(`    result.push({ ${fields.map(f => f.name).join(', ')} })`)
    source.push('    off += 7')
    source.push('  }')
    source.push('  return result')
    const read = source.join('\n')

    source.length = 0
    if (params.length) {
      source.push('const { bindings, exec } = this')
      source.push('const { view, buffer } = exec')
      source.push('let next = 0')
      source.push('for (let args of batchArgs) {')
      source.push('  const { paramStart } = bindings[next + first]')
      source.push('  let off = paramStart')
      for (let i = 0; i < params.length; i++) {
        if ((formats[i] || formats[0]).oid === INT4OID) {
          if ((formats[i] || formats[0]).format === constants.formats.Binary) {
            if (params.length === 1) {
              source.push('  view.setUint32(off + 4, args)')
            } else {
              source.push(`  view.setUint32(off + 4, args[${i}])`)
            }
            source.push('  off += 8')
          } else {
            if (params.length === 1) {
              source.push('  const val = args.toString()')
            } else {
              source.push(`  const val = args[${i}]).toString()`)
            }
            source.push('  view.setUint32(off, val.length)')
            source.push('  off += 4')
            source.push('  off += buffer.writeString(val, off)')
          }
        } else {
          if ((formats[i] || formats[0]).format === constants.formats.Binary) {
            if (params.length === 1) {
              source.push('  const paramString = args.toString()')
            } else {
              source.push(`  const paramString = args[${i}].toString()`)
            }
            source.push('  view.setUint32(paramStart, paramString.length)')
            source.push('  off += 4')
            source.push('  off += buffer.writeString(paramString, off)')
          } else {
            if (params.length === 1) {
              source.push('  const buf = args')
            } else {
              source.push(`  const buf = args[${i}]`)
            }
            source.push('  view.setUint32(paramStart, buf.byteLength)')
            source.push('  off += 4')
            source.push('  off += buffer.copyFrom(buf, off, buf.byteLength, 0)')
          }
        }
      }
      source.push('  next++')
      source.push('}')
    }
    const writeBatch = source.join('\n')

    source.length = 0
    if (params.length) {
      source.push('const { exec, query } = this')
      source.push('const { params } = query')
      source.push('const { view, buffer } = exec')
      for (let i = 0; i < params.length; i++) {
        if ((formats[i] || formats[0]).oid === INT4OID) {
          if ((formats[i] || formats[0]).format === constants.formats.Binary) {
            source.push(`view.setUint32(off + 4, params[${i}])`)
            source.push('off += 8')
          } else {
            source.push(`const val = params[${i}]).toString()`)
            source.push('view.setUint32(off, val.length)')
            source.push('off += 4')
            source.push('off += buffer.writeString(val, off)')
          }
        } else {
          if ((formats[i] || formats[0]).format === constants.formats.Binary) {
            source.push(`const buf = params[${i}]`)
            source.push('view.setUint32(off, buf.byteLength)')
            source.push('off += 4')
            source.push('off += buffer.copyFrom(buf, off, buf.byteLength, 0)')
          } else {
            source.push(`const paramString = params[${i}].toString()`)
            source.push('view.setUint32(off, paramString.length)')
            source.push('off += 4')
            source.push('off += buffer.writeString(paramString, off)')
          }
        }
      }
    }
    const writeSingle = source.join('\n')
    this.source = { read, writeBatch, writeSingle }
    return this
  }

  write (args, first) {}
  writeSingle (off) {}

  // read the current rows from the buffers - to be overridden
  read () {
    return []
  }

  runBatch (args = [[]]) {
    const batch = this
    const { sock, size, bindings, sync, exec } = this
    const { callbacks } = sock
    const { buffer } = exec
    const n = args.length
    const first = size - n
    batch.writeBatch(args, first)
    const start = bindings[first].offsets.start
    if (this.syncing >= this.pending) {
      sock.append(buffer, exec.off - start, start, true)
      this.syncing = 0
    } else {
      sock.append(buffer, exec.off - 5 - start, start, false)
      this.syncing++
    }
    this.pending++
    const results = []
    let done = 0
    return new Promise((resolve, reject) => {
      args.forEach(args => callbacks.push((err, rows) => {
        // todo: what about the rest of the inflight callbacks?
        if (err) {
          reject(err)
          return
        }
        // read the results from this query
        if (rows === 0) {
          resolve()
          return
        }
        if (n === 1) {
          batch.pending--
          if (batch.syncing > 0 && batch.syncing >= batch.pending) {
            sock.append(sync.buffer, sync.off, 0, true)
            batch.syncing = 0
          }
          resolve([batch.read()])
          return
        }
        results.push(batch.read())
        if (++done === n) {
          batch.pending--
          if (batch.syncing > 0 && batch.syncing >= batch.pending) {
            sock.append(sync.buffer, sync.off, 0, true)
            batch.syncing = 0
          }
          resolve(results)
        }
      }))
    })
  }

  runSingle (commit = false) {
    const batch = this
    const { sock, exec, sync } = batch
    const binding = batch.bindings[batch.syncing]
    const first = batch.bindings[0]
    const { callbacks } = sock
    const { buffer } = exec
    const { paramStart } = binding
    batch.writeSingle(paramStart)
    const start = first.offsets.start
    if (commit || this.syncing >= this.pending || this.syncing === 64) {
      sock.append(buffer, binding.eoff - start, start, false)
      sock.append(sync.buffer, sync.off, 0, true)
      this.syncing = 0
    } else {
      this.syncing++
    }
    this.pending++
    return new Promise((resolve, reject) => {
      callbacks.push((err, rows) => {
        batch.pending--
        if (batch.syncing > 0 && batch.syncing >= batch.pending) {
          const binding = batch.bindings[batch.syncing - 1]
          sock.append(buffer, binding.eoff - start, start, false)
          sock.append(sync.buffer, sync.off, 0, true)
          batch.syncing = 0
        }
        if (err) {
          reject(err)
          return
        }
        if (rows === 0) {
          resolve()
          return
        }
        resolve(batch.read())
      })
    })
  }
}

function startupMessage (db) {
  const { user, database, version, parameters = [] } = db
  let len = 8 + 4 + 1 + user.length + 1 + 8 + 1 + database.length + 2
  for (let i = 0; i < parameters.length; i++) {
    const { name, value } = parameters[i]
    len += (name.length + 1 + value.length + 1)
  }
  const buf = new ArrayBuffer(len)
  const dv = new DataView(buf)
  let off = 0
  dv.setInt32(0, 0)
  off += 4
  dv.setInt32(4, version) // protocol version
  off += 4
  off += buf.writeString('user', off)
  dv.setUint8(off++, 0)
  off += buf.writeString(user, off)
  dv.setUint8(off++, 0)
  off += buf.writeString('database', off)
  dv.setUint8(off++, 0)
  off += buf.writeString(database, off)
  dv.setUint8(off++, 0)
  for (let i = 0; i < parameters.length; i++) {
    const { name, value } = parameters[i]
    off += buf.writeString(name, off)
    dv.setUint8(off++, 0)
    off += buf.writeString(value, off)
    dv.setUint8(off++, 0)
  }
  dv.setUint8(off++, 0)
  dv.setInt32(0, off)
  return buf
}

function md5AuthMessage ({ user, pass, salt }) {
  const token = `${pass}${user}`
  let hash = md5(token)
  const plain = new ArrayBuffer(36)
  plain.writeString(`md5${hash}`, 0)
  const plain2 = new ArrayBuffer(36)
  plain2.copyFrom(plain, 0, 32, 3)
  plain2.copyFrom(salt, 32, 4)
  hash = `md5${md5(plain2)}`
  const len = hash.length + 5
  let off = 0
  const buf = new ArrayBuffer(len + 1)
  const dv = new DataView(buf)
  dv.setUint8(off++, 112)
  dv.setUint32(off, len)
  off += 4
  off += buf.writeString(hash, off)
  dv.setUint8(off++, 0)
  return buf
}

// ACTIONS - Async/Promise Interface
// todo - turn this into a PGSocket class that extends Socket from TCP
function authenticate (sock, salt) {
  const { user, pass } = sock.config
  // todo: check all return codes!
  sock.write(md5AuthMessage({ user, pass, salt }))
  return new Promise((resolve, reject) => {
    sock.callbacks.push(err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function start (sock) {
  sock.write(startupMessage(sock.config))
  return new Promise((resolve, reject) => {
    sock.callbacks.push(err => {
      if (err) return reject(err)
      resolve()
    })
  })
}

function connectSocket (config, buffer) {
  const { hostname, port } = config
  return new Promise((resolve, reject) => {
    lookup(hostname, (err, ip) => {
      if (err) {
        reject(err)
        return
      }
      let connected = false
      config.address = ip
      const sock = createClient(ip, port)
      sock.onClose = () => {
        if (!connected) {
          reject(new Error('Could Not Connect'))
          return
        }
        // what to do if connection drops? retries?
      }
      sock.onConnect = err => {
        if (err) {
          reject(err)
          return
        }
        connected = true
        if (config.noDelay) sock.setNoDelay()
        resolve(sock)
        return buffer
      }
      sock.buffer = buffer
      sock.connect()
    })
  })
}

async function createConnection (config) {
  if (!config.bufferSize) config.bufferSize = 64 * 1024
  if (!config.version) config.version = constants.PG_VERSION
  if (!config.port) config.port = 5432
  const callbacks = []

  const sock = await connectSocket(config, new ArrayBuffer(config.bufferSize))
  sock.config = config
  sock.callbacks = callbacks
  sock.authenticated = false

  const defaultAction = (...args) => callbacks.shift()(...args)
  const defaults = [CloseComplete, AuthenticationOk, ParseComplete, NoData]
  const actions = {}
  defaults.forEach(type => {
    actions[type] = defaultAction
  })
  actions[BackendKeyData] = () => {}
  actions[BindComplete] = () => {}
  actions[ReadyForQuery] = () => {
    if (sock.authenticated) return
    sock.authenticated = true
    defaultAction()
  }
  actions[ErrorResponse] = () => {
    defaultAction(new Error(JSON.stringify(parser.errors, null, '  ')))
  }
  actions[CommandComplete] = () => defaultAction(null, parser.state.rows)
  actions[RowDescription] = () => {
    if (sock.callbacks[0].isExec) return
    defaultAction()
  }

  const parser = createParser(sock.buffer)
  parser.onMessage = () => {
    const { type } = parser
    return actions[type]()
  }

  sock.create = (config, size = 1) => {
    const query = Object.assign({}, config)
    if (!query.portal) query.portal = ''
    query.params = Array(config.params || 0).fill(0)
    if (!query.fields) query.fields = []
    if (!query.formats) query.formats = []
    if (!query.maxRows) query.maxRows = 0
    return (new Query(sock, query, size)).setup().generate().compile().create()
  }

  const buffer = new ArrayBuffer(64 * 1024)
  const size = buffer.byteLength
  let offset = 0
  const u8 = new Uint8Array(buffer)

  sock.append = (buf, len, off, flush = false) => {
    if (offset + len > size) {
      sock.write(buffer, offset, 0)
      offset = 0
    }
    u8.set(new Uint8Array(buf, off, len), offset)
    offset += len
    if (flush) {
      sock.write(buffer, offset, 0)
      offset = 0
    }
  }

  // todo: backpressure - pause/resume
  sock.onData = bytes => parser.parse(bytes)
  sock.parser = parser

  await start(sock)
  await authenticate(sock, parser.salt)
  return sock
}

function connect (config, poolSize = 1) {
  const connections = []
  for (let i = 0; i < poolSize; i++) {
    connections.push(createConnection(config))
  }
  return Promise.all(connections)
}

/**
 * Generate a Bulk Update SQL statement definition which can be passed to
 * sock.create. For a given table, identity column and column to be updated, it
 * will generate a single SQL statement to update all fields in one statement
 *
 * @param {string} table   - The name of the table
 * @param {string} field   - The name of the field we want to update
 * @param {string} id      - The name of the id field
 * @param {string} updates - The number of rows to update in the statement
 * @param {string} type    - The name of the table
 */
function generateBulkUpdate (table, field, id, updates = 5, type = constants.BinaryInt) {
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
  const formats = [type]
  const sql = []
  sql.push(`update ${table} set ${field} = CASE ${id}`)
  sql.push(getClauses(updates))
  sql.push(`else ${field}`)
  sql.push(`end where ${id} in (${getIds(updates)})`)
  return { formats, name: `${updates}`, params: updates * 2, sql: sql.join('\n') }
}

module.exports = { constants, connect, generateBulkUpdate, createParser }
