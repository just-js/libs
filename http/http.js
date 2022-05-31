const { http } = just.library('http')
const { encode } = just.library('encode')
const { sha1 } = just.library('sha1')
const { createSocket, Socket } = require('@socket')
const websocket = require('websocket')
const parsers = require('parsers.js')

const { hash } = sha1
const { createMessage, createBinaryMessage, unmask } = websocket
const {
  parseRequestsHandle,
  parseResponsesHandle,
  createHandle,
  getStatusCode,
  getStatusMessage,
  getMethod,
  getUrl,
  getHeaders
} = http
const messageTypes = {
  Request: 0,
  Response: 1
}

class HTTPParser {
  constructor (buffer, type = messageTypes.Request) {
    this.buffer = buffer
    this.type = type
    this.info = new ArrayBuffer(4)
    this.dv = new DataView(this.info)
    this.handle = createHandle(this.buffer, this.info)
    this.remaining = 0
    this.count = 0
    this.index = 0
    this.parseHandle = this.type === messageTypes.Request ? parseRequestsHandle : parseResponsesHandle
  }

  get request () {
    return new IncomingRequest(this.index)
  }

  get response () {
    return new IncomingResponse(this.index)
  }

  parse (bytes, off = 0) {
    const { handle, dv } = this
    this.parseHandle(handle, bytes, off)
    const flags = dv.getUint32(0, true)
    this.count = flags & 0xffff
    this.remaining = flags >> 16
    this.index = 0
    if (this.count > 0) return true
    return false
  }

  reset () {
    this.remaining = this.count = this.index = 0
  }
}

class URL {
  static parse (url) {
    const protocolEnd = url.indexOf(':')
    const protocol = url.slice(0, protocolEnd)
    const hostnameEnd = url.indexOf('/', protocolEnd + 3)
    const host = url.slice(protocolEnd + 3, hostnameEnd)
    const path = url.slice(hostnameEnd)
    const [hostname, port] = host.split(':')
    if (port) return { protocol, host, hostname, path, port }
    return { protocol, host, hostname, path, port: protocol === 'https' ? 443 : 80 }
  }
}

class OutgoingRequest {
  constructor () {
    
  }
}

function joinHeaders (headers) {
  return headers.map(h => h.join(': ')).join('\r\n')
}

class OutgoingResponse {
  constructor (sock) {
    this.sock = sock
    this.pipeline = false
    this.queue = ''
    this.status = 200
    this.headers = []
    this.contentType = 'text'
    this.chunked = false
    this.contentLength = 0
    this.close = false
    this.index = 0
    this.request = new IncomingRequest(this.index)
  }

  raw (buf) {
    return this.sock.send(buf)
  }

  header (size, contentType = responses.octet) {
    const { sock, status, headers } = this
    if (headers.length) {
      if (size === 0) {
        sock.sendString(`${contentType[status].slice(0, -16)}${joinHeaders(headers)}${END}`)
      } else {
        sock.sendString(`${contentType[status]}${size}${CRLF}${joinHeaders(headers)}${END}`)
      }
    } else {
      if (size === 0) {
        sock.sendString(`${contentType[status.slice(0, -16)]}${END}`)
      } else {
        sock.sendString(`${contentType[status]}${size}${END}`)
      }
    }
  }

  buffer (buf, contentType = responses.octet) {
    const { sock, status, headers } = this
    if (headers.length) {
      sock.sendString(`${contentType[status]}${buf.byteLength}${CRLF}${joinHeaders(headers)}${END}`)
    } else {
      sock.sendString(`${contentType[status]}${buf.byteLength}${END}`)
    }
    sock.send(buf)
  }

  json (json, contentType = responses.json) {
    const str = JSON.stringify(json)
    const { sock, pipeline, status, headers } = this
    if (pipeline) {
      this.queue += `${contentType[status]}${String.byteLength(str)}${END}${str}`
      return
    }
    if (headers.length) {
      return sock.sendString(`${contentType[status]}${String.byteLength(str)}${CRLF}${joinHeaders(headers)}${END}${str}`)
    }
    return sock.sendString(`${contentType[status]}${String.byteLength(str)}${END}${str}`)
  }

  utf8 (str, contentType = responses.utf8) {
    if (this.pipeline) {
      this.queue += `${contentType[this.status]}${String.byteLength(str)}${END}${str}`
      return
    }
    if (this.headers.length) {
      return this.sock.sendString(`${contentType[this.status]}${String.byteLength(str)}${CRLF}${joinHeaders(this.headers)}${END}${str}`)
    }
    return this.sock.sendString(`${contentType[this.status]}${String.byteLength(str)}${END}${str}`)
  }

  html (str, contentType = responses.html) {
    if (this.pipeline) {
      this.queue += `${contentType[this.status]}${String.byteLength(str)}${END}${str}`
      return
    }
    if (this.headers.length) {
      return this.sock.sendString(`${contentType[this.status]}${String.byteLength(str)}${CRLF}${joinHeaders(this.headers)}${END}${str}`)
    }
    return this.sock.sendString(`${contentType[this.status]}${String.byteLength(str)}${END}${str}`)
  }

  notFound () {
    this.status = 404
    this.text('Not Found')
  }

  text (str, contentType = responses.text) {
    if (this.pipeline) {
      this.queue += `${contentType[this.status]}${str.length}${END}${str}`
      return
    }
    if (this.headers.length) {
      return this.sock.sendString(`${contentType[this.status]}${str.length}${CRLF}${joinHeaders(this.headers)}${END}${str}`)
    }
    return this.sock.sendString(`${contentType[this.status]}${str.length}${END}${str}`)
  }

  finish () {
    if (this.pipeline && this.queue.length) {
      this.sock.sendString(this.queue)
      this.queue = ''
      this.pipeline = false
    }
  }
}

class IncomingResponse {
  constructor (index) {
    this.index = index
    this.hasHeaders = false
    this._headers = {}
    this.version = 0
  }

  get statusCode () {
    return getStatusCode(this.index)
  }

  get statusMessage () {
    return getStatusMessage(this.index)
  }

  get headers () {
    if (this.hasHeaders) return this._headers
    this.version = getHeaders(this.index, this._headers)
    this.hasHeaders = true
    return this._headers
  }
}

class IncomingRequest {
  constructor (index) {
    this.index = index
    this.params = []
    this.hasHeaders = false
    this._headers = {}
    this.version = 0
    this.qs = ''
    this.path = ''
    this.query = null
    this.contentLength = 0
    this.bytes = 0
    this.url = ''
    this.chunked = false
    this.method = methods.get
    this.onBody = (buf, len) => {}
    this.onEnd = () => {}
  }

  get headers () {
    if (this.hasHeaders) return this._headers
    this.version = getHeaders(this.index, this._headers)
    this.hasHeaders = true
    return this._headers
  }

  json () {
    const req = this
    let str = ''
    req.onBody = (buf, len, off) => {
      str += buf.readString(len, off)
    }
    return new Promise(resolve => {
      req.onEnd = () => resolve(JSON.parse(str))
    })
  }

  text () {
    const req = this
    let str = ''
    req.onBody = (buf, len, off) => {
      str += buf.readString(len, off)
    }
    return new Promise(resolve => {
      req.onEnd = () => resolve(str)
    })
  }

  parseUrl (qs = false) {
    if (!qs && this.path) return this
    if (qs && this.query) return this
    const { url } = this
    const i = url.indexOf(PATHSEP)
    if (i > -1) {
      this.path = url.slice(0, i)
      this.qs = url.slice(i + 1)
    } else {
      this.path = url
      this.qs = ''
    }
    if (qs) {
      // parse the querystring
      if (!this.qs) return this
      this.query = this.qs.split('&')
        .map(p => p.split('='))
        .reduce((o, p) => {
          o[p[0]] = p[1]
          return o
        }, {})
    }
    return this
  }
}

class ChunkParser {
  constructor (buf) {
    this.size = 0
    this.consumed = 0
    this.buffer = buf
    this.bytes = new Uint8Array(buf)
    this.digits = []
    this.header = true
    this.final = false
  }

  reset () {
    this.size = 0
    this.consumed = 0
    this.digits.length = 0
    this.final = false
    this.header = true
  }

  parse (len, start) {
    const { bytes, digits } = this
    let off = start
    const chunks = []
    while (len) {
      if (this.header) {
        const c = bytes[off]
        off++
        len--
        if (c === 13) {
          continue
        } else if (c === 10) {
          if (this.final) {
            this.reset()
            return
          }
          if (digits.length) {
            this.size = parseInt(digits.join(''), 16)
            if (this.size > 0) {
              this.header = false
            } else if (this.size === 0) {
              this.final = true
            }
            digits.length = 0
          }
          continue
        } else if ((c > 47 && c < 58)) {
          digits.push(String.fromCharCode(c))
          continue
        } else if ((c > 96 && c < 103)) {
          digits.push(String.fromCharCode(c))
          continue
        } else if ((c > 64 && c < 71)) {
          digits.push(String.fromCharCode(c))
          continue
        } else {
          just.print('BAD_CHAR')
        }
        just.print('OOB:')
        just.print(`c ${c}`)
        just.print(`len ${len}`)
        just.print(`off ${off}`)
        just.print(`size ${this.size}`)
        just.print(`consumed ${this.consumed}`)
        throw new Error('OOB')
      } else {
        const remaining = this.size - this.consumed
        if (remaining > len) {
          chunks.push(this.buffer.slice(off, off + len))
          this.consumed += len
          off += len
          len = 0
        } else {
          chunks.push(this.buffer.slice(off, off + remaining))
          len -= remaining
          off += remaining
          this.consumed += remaining
          this.reset()
        }
      }
    }
    return chunks
  }
}

function createResponses (serverName) {
  // todo: expose this so it can be configured
  time = (new Date()).toUTCString()
  Object.keys(contentTypes).forEach(contentType => {
    Object.keys(statusMessages).forEach(status => {
      responses[contentType][status] = `HTTP/1.1 ${status} ${statusMessages[status]}\r\nServer: ${serverName}\r\nContent-Type: ${contentTypes[contentType]}\r\nDate: ${time}\r\nContent-Length: `
    })
  })
}

const random = v => Math.ceil(Math.random() * v)
const randomSleep = () => new Promise(resolve => just.setTimeout(resolve, random(300) + 20))

async function createServerSocket (sock, server) {
  const { staticHandlers, defaultHandler, hooks } = server
  const { buffer } = sock
  if (server.tls) {
    await sock.negotiate(server.context)
    if(sock.error) {
      just.print(sock.error.stack)
      sock.close()
      return
    }
  }
  sock.edgeTriggered = false
  const parser = new HTTPParser(buffer)
  if (hooks.connect.length) {
    for (const handler of hooks.connect) handler(sock)
  }
  if (hooks.disconnect.length) {
    sock.onClose = () => {
      for (const handler of hooks.disconnect) handler(sock)
    }
  }
  sock.pause()
  await randomSleep()
  let offset = 0
  sock.onReadable = () => {
    const bytes = sock.recv(offset)
    if (bytes === 0) {
      sock.close()
      return
    }
    if (bytes > 0) {
      if (parser.parse(offset + bytes)) {
        const { count } = parser
        const res = new OutgoingResponse(sock)
        res.pipeline = count > 1
        for (let i = 0; i < count; i++) {
          res.index = i
          const method = getMethod(i)
          res.request.method = method
          const methodHandler = staticHandlers[method]
          if (!methodHandler) {
            const url = getUrl(i)
            const { request } = res
            request.url = url
            defaultHandler(res, res.request)
            continue
          }
          const url = getUrl(i)
          const { request } = res
          request.url = url
          let handler = methodHandler[url]
          if (handler) {
            const p = handler(res, request)
            if (handler.opts && handler.opts.async) {
              p.catch(err => server.error(res, err))
            }
            continue
          }
          request.parseUrl(true)
          handler = methodHandler[request.path]
          if (handler) {
            const p = handler(res, request)
            if (handler.opts && handler.opts.async) {
              p.catch(err => server.error(res, err))
            }
            continue
          }
          defaultHandler(res, request)
        }
        const { remaining } = parser
        if (remaining > 0) {
          parser.buffer.copyFrom(parser.buffer, 0, remaining, offset + bytes - remaining)
          offset = remaining
        } else {
          //for (const handler of hooks.post) handler(res)
          offset = 0
        }
        res.finish()
      } else {
        if (!sock.upgraded) {
          const { remaining } = parser
          if (remaining > 0) {
            offset += remaining
          } else {
            offset = 0
          }
        }
      }
    } else {
      if (!sock.blocked) sock.close()
    }
  }
  sock.resume()
}

// todo: change this to be like pg where we pass the socket in
class Server {
  constructor (sock = createSocket()) {
    this.sock = sock
    this.staticHandlers = {}
    this.regexHandlers = {}
    this.defaultHandler = this.notFound
    this.hooks = { connect: [], disconnect: [], post: [], pre: [] }
    this.accepting = false
    this.stackTraces = false
    this.tls = false
    this.context = null
    this.name = 'j'
  }

  async start () {
    let sock = await this.sock.accept()
    while (sock && this.accepting) {
      createServerSocket(sock, this)
      // todo handle rejection
      sock = await this.sock.accept()
    }
  }

  stop () {
    this.accepting = false
  }

  close () {
    this.stop()
    return this.sock.close()
  }

  connect (handler) {
    this.hooks.connect.push(handler)
    return this
  }

  disconnect (handler) {
    this.hooks.disconnect.push(handler)
    return this
  }

  match (url, method) {
    for (const handler of this.regexHandlers[method]) {
      const match = url.match(handler.path)
      if (match) {
        return [handler.handler, match.slice(1)]
      }
    }
    return [null, null]
  }

  use (handler, post = false) {
    if (post) {
      this.hooks.post.push(handler)
      return this
    }
    this.hooks.pre.push(handler)
    return this
  }

  notFound (res) {
    res.status = 404
    res.text('Not Found')
  }

  error (res, err) {
    res.status = 500
    if (this.stackTraces) {
      res.text(`
error: ${err.toString()}
stack:
${err.stack}
`)
      return
    }
    res.text(err.toString())
  }

  addPath (path, handler, method, opts) {
    if (opts) handler.opts = opts
    if (!this.staticHandlers[method]) this.staticHandlers[method] = {}
    if (!this.regexHandlers[method]) this.regexHandlers[method] = []
    if (handler.constructor.name === 'AsyncFunction') {
      if (handler.opts) {
        handler.opts.async = true
      } else {
        handler.opts = { async: true }
      }
    }
    if (typeof path === 'string') {
      this.staticHandlers[method][path] = handler
      return
    }
    if (typeof path === 'object') {
      if (path.constructor.name === 'RegExp') {
        this.regexHandlers[method].push({ handler, path })
      } else if (path.constructor.name === 'Array') {
        for (const p of path) {
          this.staticHandlers[method][p] = handler
        }
      }
    }
    return this
  }

  removePath (method, path) {
    // this only works for staticHandlers as we cannot compared to regex
    delete this.staticHandlers[method][path]
  }

  get (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, methods.get)
    return this
  }

  put (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, methods.put)
    return this
  }

  post (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, methods.post)
    return this
  }

  delete (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, methods.delete)
    return this
  }

  options (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, methods.options)
    return this
  }

  default (handler, opts) {
    if (opts) handler.opts = opts
    this.defaultHandler = handler
    return this
  }

  listen (address = '127.0.0.1', port = 3000, maxConn = this.sock.maxConn) {
    this.sock.maxConn = maxConn
    const ok = this.sock.listen(address, port)
    if (ok !== 0) throw new just.SystemError('listen')
    this.accepting = true
    this.start()
    return this
  }

  update () {
    createResponses(this.name)
    return this
  }
}

function getResponses (count = 1) {
  if (count === 0) return []
  const responses = [[]]
  http.getResponses(count, responses)
  return responses.map(res => {
    const [version, statusCode, statusMessage, headers] = res
    res.chunked = false
    return { version, statusCode, statusMessage, headers }
  })
}

function getRequests (count = 1) {
  if (count === 0) return []
  const requests = [[]]
  http.getRequests(count, requests)
  return requests.map(req => {
    const [path, version, methodLen, headers] = req
    return { path, version, methodLen, headers }
  })
}

function createServer (opts = {}, sock = createSocket()) {
  const server = new Server(sock)
  if (opts.tls) server.tls = true
  return server.update()
}

function joinHeaders2 (headers) {
  return `${Object.keys(headers).map(k => [k, headers[k]].join(': ')).join('\r\n')}\r\n`
}

function createRequest (path, hostname, headers) {
  if (headers) {
    return ArrayBuffer.fromString(`GET ${path} HTTP/1.1\r\nHost: ${hostname}\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n${joinHeaders2(headers)}\r\n`)
  }
  return ArrayBuffer.fromString(`GET ${path} HTTP/1.1\r\nConnection: close\r\nHost: ${hostname}\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n`)
}

async function fetchJSON (sock, path, hostname, headers, onHeaders, onBody, onComplete) {
  await sock.push(createRequest(path, hostname, headers))
  const info = new ArrayBuffer(4)
  const dv = new DataView(info)
  const handle = createHandle(sock.buffer, info)
  let inBody = false
  let res
  //let rawHeaders
  let parser
  let expectedLength = 0
  let bytes = await sock.pull()
  const body = []
  while (bytes) {
    if (!inBody) {
      parseResponsesHandle(handle, bytes, 0)
      const r = dv.getUint32(0, true)
      const count = r & 0xff
      const remaining = r >> 16
      if (count > 0) {
        //rawHeaders = sock.buffer.readString(bytes - remaining, 0)
        //just.print(rawHeaders)
        res = getResponses()[0]
        res.sock = sock
        if (onHeaders) onHeaders(res.headers)
        res.chunked = (res.headers['Transfer-Encoding'] && res.headers['Transfer-Encoding'].toLowerCase() === 'chunked')
        res.bytes = 0
        if (res.chunked) {
          parser = new ChunkParser(sock.buffer)
          const chunks = parser.parse(remaining, bytes - remaining)
          if (chunks && chunks.length) {
            res.bytes += chunks.reduce((size, chunk) => size + chunk.byteLength, 0)
            for (const chunk of chunks) {
              if (onBody) {
                onBody(chunk)
              } else {
                body.push(chunk.readString())
              }
            }
          }
          inBody = true
        } else {
          expectedLength = parseInt(res.headers['Content-Length'] || res.headers['content-length'], 10)
          //just.print(`expected ${expectedLength} remaining ${remaining}`)
          if (remaining > 0) {
            res.bytes += remaining
            if (onBody) {
              onBody(sock.buffer.slice(remaining, bytes))
            } else {
              body.push(sock.buffer.readString(remaining, bytes - remaining))
            }
          }
          //just.print(res.bytes)
          if (expectedLength === res.bytes) break
        }
      } else {
        just.print(`count ${count} remaining ${remaining}`)
      }
    } else {
      if (res.chunked) {
        const chunks = parser.parse(bytes, 0)
        if (chunks && chunks.length) {
          res.bytes += chunks.reduce((size, chunk) => size + chunk.byteLength, 0)
          for (const chunk of chunks) {
            if (onBody) {
              onBody(chunk)
            } else {
              body.push(chunk.readString())
            }
          }
        }
      } else {
        if (onBody) {
          onBody(sock.buffer.slice(0, bytes))
        } else {
          body.push(sock.buffer.readString(bytes))
        }
        res.bytes += bytes
        if (expectedLength === res.bytes) break
      }
    }
    bytes = await sock.pull()
  }
  if (onComplete) {
    onComplete()
  } else {
    res.body = body.join('')
  }
  //sock.close()
  return res
}

function shasum (str) {
  const source = ArrayBuffer.fromString(str)
  const dest = new ArrayBuffer(20)
  return source.readString(encode.base64Encode(dest, source, hash(source, dest)))
}

Socket.prototype.message = function (str) {
  return this.send(createMessage(str))
}

Socket.prototype.upgrade = function (res, onMessage = () => {}, onClose = () => {}) {
  const { sock, request } = res
  const { buffer } = sock
  const key = request.headers['Sec-WebSocket-Key']
  const hash = shasum(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)      
  res.headers.push(['Upgrade', 'WebSocket'])
  res.headers.push(['Connection', 'Upgrade'])
  res.headers.push(['Sec-WebSocket-Accept', hash])
  res.status = 101
  res.text('')
  const parser = new websocket.Parser()
  const chunks = []
  parser.onHeader = header => {
    chunks.length = 0
  }
  parser.onChunk = (off, len, header) => {
    let size = len
    let pos = 0
    const bytes = new Uint8Array(buffer, off, len)
    while (size--) {
      bytes[pos] = bytes[pos] ^ header.maskkey[pos % 4]
      pos++
    }
    chunks.push(buffer.readString(len, off))
  }
  parser.onMessage = header => {
    if (header.OpCode === 8) {
      onClose()
      chunks.length = 0
      return
    }
    const str = chunks.join('')
    if (!str) return
    chunks.length = 0
    onMessage(str)
  }
  const u8 = new Uint8Array(buffer)
  sock.upgraded = true
  sock.onBytes = bytes => {
    parser.execute(u8, 0, bytes)
  }
}

const statusMessages = {
  200: 'OK',
  201: 'Created',
  204: 'OK',
  101: 'Switching Protocols',
  400: 'Bad Request',
  401: 'Unauthorized',
  403: 'Forbidden',
  404: 'Not Found',
  429: 'Server Busy',
  500: 'Server Error'
}

const methods = {
  get: 'GET'.charCodeAt(0),
  put: 'PUT'.charCodeAt(0),
  post: 'POST'.charCodeAt(0), // TODO: PUT and POST are same
  delete: 'DELETE'.charCodeAt(0),
  options: 'OPTIONS'.charCodeAt(0)
}

const contentTypes = {
  text: 'text/plain',
  css: 'text/css',
  utf8: 'text/plain; charset=utf-8',
  json: 'application/json; charset=utf-8',
  html: 'text/html; charset=utf-8',
  octet: 'application/octet-stream',
  ico: 'application/favicon',
  png: 'application/png',
  xml: 'application/xml; charset=utf-8',
  js: 'application/javascript; charset=utf-8',
  wasm: 'application/wasm'
}

const responses = {}
Object.keys(contentTypes).forEach(k => responses[k] = {})

const END = '\r\n\r\n'
const CRLF = '\r\n'
const PATHSEP = '?'

let time = (new Date()).toUTCString()

module.exports = {
  URL,
  Server,
  ChunkParser,
  IncomingRequest,
  IncomingResponse,
  OutgoingRequest,
  OutgoingResponse,
  HTTPParser,
  messageTypes,
  contentTypes,
  statusMessages,
  responses,
  createServer,
  getResponses,
  getRequests,
  fetchJSON,
  parsers
}
