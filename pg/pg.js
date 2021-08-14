const { createClient } = require('@tcp')
const { lookup } = require('@dns')
const md5 = require('@md5')
const { html } = just.library('html')

// Constants
const PG_VERSION = 0x00030000

const constants = {
  AuthenticationMD5Password: 5,
  formats: {
    Text: 0,
    Binary: 1
  },
  fieldTypes: {
    INT4OID: 23,
    VARCHAROID: 1043
  },
  messageTypes: {
    AuthenticationOk: 82,
    ErrorResponse: 69,
    RowDescription: 84,
    CommandComplete: 67,
    ParseComplete: 49,
    CloseComplete: 51,
    BindComplete: 50,
    ReadyForQuery: 90,
    BackendKeyData: 75,
    ParameterStatus: 83,
    ParameterDescription: 116,
    DataRow: 68,
    NoData: 110
  }
}

const { INT4OID, VARCHAROID } = constants.fieldTypes
const messageNames = {}
Object.keys(constants.messageTypes).forEach(k => {
  messageNames[constants.messageTypes[k]] = k
})
constants.messageNames = messageNames

constants.BinaryInt = {
  format: constants.formats.Binary,
  oid: INT4OID
}

constants.VarChar = {
  format: constants.formats.Text,
  oid: VARCHAROID
}

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

// Utilities
function readCString (buf, u8, off) {
  const start = off
  while (u8[off] !== 0) off++
  return buf.readString(off - start, start)
}

// Protocol
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
    this.htmlEscape = html.escape
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
    const bindExecLen = flushLen + syncLen + (size * (bindLen + execLen))
    const prepareDescribeLen = prepareLen + describeLen + flushLen
    const m = new Messaging(new ArrayBuffer(bindExecLen), query)
    for (let i = 0; i < size; i++) {
      const bind = m.createBindMessage()
      bindings.push(bind)
      m.off = bind.off
      const exec = m.createExecMessage()
      m.off = exec.off
    }
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

    this.exec = m
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
    if (writeSingle.length) this.writeSingle = just.vm.compile(writeSingle, `${query.name}s.js`, [], [])
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
      source.push('const { bindings, exec, query, size } = this')
      source.push('const { params } = query')
      source.push('const { view, buffer } = exec')
      source.push('let off = bindings[size - 1].paramStart')
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

  // read the current rows from the buffers - to be overridden
  read () {
    return []
  }

  // the 1 or more of the batch queries, passing in an array of arguments
  // for each query we are running. args = [[a,b],[a,c],[b.c]] or [a,b,c]
  runBatch (args = [[]]) {
    const batch = this
    const { sock, size, bindings, exec } = this
    const { callbacks } = sock
    const { buffer, off } = exec
    const n = args.length
    const first = size - n
    // write the arguments into the query buffers
    batch.writeBatch(args, first)
    // write the queries onto the wire
    let r = 0
    if (n < size) {
      const start = bindings[first].offsets.start
      r = sock.write(buffer, exec.off - start, start)
    } else {
      r = sock.write(buffer, off, 0)
    }
    if (r <= 0) return Promise.reject(new Error('Bad Write'))
    const results = []
    let done = 0
    return new Promise((resolve, reject) => {
      args.forEach(args => callbacks.push((err, rows) => {
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
          resolve([batch.read()])
          return
        }
        results.push(batch.read())
        if (++done === n) resolve(results)
      }))
    })
  }

  commit () {
    const { sock, sync } = this
    const { buffer } = sync
    return sock.write(buffer, sync.off, 0)
  }

  purge () {
    const { sock, flush } = this
    const { buffer } = flush
    return sock.write(buffer, flush.off, 0)
  }

  runSingle () {
    const batch = this
    const { sock, size, exec, bindings } = batch
    const { callbacks } = sock
    const { buffer } = exec
    if (batch.writeSingle) batch.writeSingle()
    const start = bindings[size - 1].offsets.start
    const r = sock.write(buffer, exec.off - start, start)
    if (r <= 0) return Promise.reject(new Error('Bad Write'))
    return new Promise((resolve, reject) => {
      callbacks.push((err, rows) => {
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

function createParser (buf) {
  let nextRow = 0
  let parseNext = 0
  let parameters = {}
  const state = { start: 0, end: 0, rows: 0, running: false }

  function onDataRow (len, off) {
    // D = DataRow
    if (nextRow === 0) state.start = off - 5
    nextRow++
    return off + len - 4
  }

  function onCommandComplete (len, off) {
    // C = CommandComplete
    state.end = off - 5
    state.rows = nextRow
    state.running = false
    off += len - 4
    nextRow = 0
    parser.onMessage()
    return off
  }

  function onCloseComplete (len, off) {
    // 3 = CloseComplete
    parser.onMessage()
    return off + len - 4
  }

  function onRowDescripton (len, off) {
    // T = RowDescription
    const fieldCount = dv.getInt16(off)
    off += 2
    fields.length = 0
    for (let i = 0; i < fieldCount; i++) {
      const name = readCString(buf, u8, off)
      off += name.length + 1
      const tid = dv.getInt32(off)
      off += 4
      const attrib = dv.getInt16(off)
      off += 2
      const oid = dv.getInt32(off)
      off += 4
      const size = dv.getInt16(off)
      off += 2
      const mod = dv.getInt32(off)
      off += 4
      const format = dv.getInt16(off)
      off += 2
      fields.push({ name, tid, attrib, oid, size, mod, format })
    }
    parser.onMessage()
    return off
  }

  function onAuthenticationOk (len, off) {
    // R = AuthenticationOk
    const method = dv.getInt32(off)
    off += 4
    if (method === constants.AuthenticationMD5Password) {
      parser.salt = buf.slice(off, off + 4)
      off += 4
      parser.onMessage()
    }
    return off
  }

  function onErrorResponse (len, off) {
    // E = ErrorResponse
    errors.length = 0
    let fieldType = u8[off++]
    while (fieldType !== 0) {
      const val = readCString(buf, u8, off)
      errors.push({ type: fieldType, val })
      off += (val.length + 1)
      fieldType = u8[off++]
    }
    parser.onMessage()
    return off
  }

  function onParameterStatus (len, off) {
    // S = ParameterStatus
    const key = readCString(buf, u8, off)
    off += (key.length + 1)
    const val = readCString(buf, u8, off)
    off += val.length + 1
    parameters[key] = val
    return off
  }

  function onParameterDescription (len, off) {
    // t = ParameterDescription
    const nparams = dv.getInt16(off)
    parser.params = []
    off += 2
    for (let i = 0; i < nparams; i++) {
      parser.params.push(dv.getUint32(off))
      off += 4
    }
    return off
  }

  function onParseComplete (len, off) {
    // 1 = ParseComplete
    off += len - 4
    parser.onMessage()
    return off
  }

  function onBindComplete (len, off) {
    // 2 = BindComplete
    off += len - 4
    parser.onMessage()
    state.rows = 0
    state.start = off
    state.running = true
    return off
  }

  function onReadyForQuery (len, off) {
    // Z = ReadyForQuery
    parser.status = u8[off]
    parser.onMessage()
    off += len - 4
    return off
  }

  function onBackendKeyData (len, off) {
    // K = BackendKeyData
    parser.pid = dv.getUint32(off)
    off += 4
    parser.key = dv.getUint32(off)
    off += 4
    parser.onMessage()
    return off
  }

  function parse (bytesRead) {
    let type
    let len
    let off = parseNext
    const end = buf.offset + bytesRead
    while (off < end) {
      const remaining = end - off
      let want = 5
      // TODO: fix this
      if (remaining < want) {
        if (byteLength - off < 1024) {
          if (state.running) {
            const queryLen = off - state.start + remaining
            just.error(`copyFrom 0 ${queryLen} ${state.start}`)
            buf.copyFrom(buf, 0, queryLen, state.start)
            buf.offset = queryLen
            parseNext = off - state.start
            state.start = 0
            return
          }
          just.error(`copyFrom 1 ${remaining} ${off}`)
          buf.copyFrom(buf, 0, remaining, off)
          buf.offset = remaining
          parseNext = 0
          return
        }
        buf.offset = off + remaining
        parseNext = off
        return
      }
      type = parser.type = dv.getUint8(off)
      len = parser.len = dv.getUint32(off + 1)
      want = len + 1
      if (remaining < want) {
        if (byteLength - off < 1024) {
          if (state.running) {
            const queryLen = off - state.start + remaining
            just.error(`copyFrom 2 ${queryLen} ${state.start} ${byteLength} ${off} ${byteLength - off}`)
            buf.copyFrom(buf, 0, queryLen, state.start)
            buf.offset = queryLen
            parseNext = off - state.start
            state.start = 0
            return
          }
          just.error(`copyFrom 3 ${remaining} ${off}`)
          buf.copyFrom(buf, 0, remaining, off)
          buf.offset = remaining
          parseNext = 0
          return
        }
        buf.offset = off + remaining
        parseNext = off
        return
      }
      off += 5
      off = (V[type] || V[0])(len, off)
    }
    parseNext = buf.offset = 0
  }

  function onDefault (len, off) {
    off += len - 4
    parser.onMessage()
    return off
  }

  function free () {
    parser.fields.length = 0
    parser.errors.length = 0
    parameters = parser.parameters = {}
    nextRow = 0
    parseNext = 0
    state.start = state.end = state.rows = 0
    state.running = false
  }

  const { messageTypes } = constants
  const dv = new DataView(buf)
  const u8 = new Uint8Array(buf)
  const byteLength = buf.byteLength
  const fields = []
  const errors = []
  const V = {
    [messageTypes.AuthenticationOk]: onAuthenticationOk,
    [messageTypes.ErrorResponse]: onErrorResponse,
    [messageTypes.RowDescription]: onRowDescripton,
    [messageTypes.CommandComplete]: onCommandComplete,
    [messageTypes.CloseComplete]: onCloseComplete,
    [messageTypes.ParseComplete]: onParseComplete,
    [messageTypes.BindComplete]: onBindComplete,
    [messageTypes.ReadyForQuery]: onReadyForQuery,
    [messageTypes.BackendKeyData]: onBackendKeyData,
    [messageTypes.ParameterStatus]: onParameterStatus,
    [messageTypes.ParameterDescription]: onParameterDescription,
    [messageTypes.DataRow]: onDataRow,
    0: onDefault
  }
  const parser = {
    buf,
    dv,
    u8,
    fields,
    parameters,
    type: 0,
    len: 0,
    errors,
    parse,
    free,
    state
  }
  return parser
}

// todo: move these to Query class
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
        // what to do if connection drops?
      }
      sock.onConnect = err => {
        if (err) {
          reject(err)
          return
        }
        connected = true
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
  if (!config.version) config.version = PG_VERSION
  if (!config.port) config.port = 5432
  const callbacks = []
  const stats = {}

  for (const k of Object.keys(constants.messageTypes)) {
    stats[constants.messageTypes[k]] = 0
  }

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
    stats[type]++
    return actions[type]()
  }
  parser.stats = () => {
    const o = Object.assign({}, stats)
    for (const k of Object.keys(stats)) stats[k] = 0
    return o
  }

  sock.create = (config, size = 1) => {
    const query = Object.assign({}, config)
    if (!query.portal) query.portal = ''
    query.params = Array(config.params || 0).fill(0)
    if (!query.fields) query.fields = []
    if (!query.formats) query.formats = []
    if (!query.maxRows) query.maxRows = 100
    return (new Query(sock, query, size)).setup().generate().compile().create()
  }
  sock.onData = parser.parse
  sock.parser = parser

  await start(sock)
  await authenticate(sock, parser.salt)
  return sock
}

function connect (config, poolSize = 1) {
  if (poolSize === 1) {
    return Promise.all([createConnection(config)])
  }
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

module.exports = { constants, connect, generateBulkUpdate }
