const { epoll } = just.library('epoll')
const { http } = just.library('http')
const { sys } = just.library('sys')
const { net } = just.library('net')

const { parseRequestsHandle, createHandle, getUrl, getMethod, getHeaders } = http
const { EPOLLIN, EPOLLERR, EPOLLHUP } = epoll
const { close, recv, accept, setsockopt, socket, bind, listen, sendString, send } = net
const { fcntl } = sys
const { loop } = just.factory
const { F_GETFL, F_SETFL } = just.sys
const { IPPROTO_TCP, O_NONBLOCK, TCP_NODELAY, SO_KEEPALIVE, SOMAXCONN, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, SO_REUSEPORT, SOCK_NONBLOCK } = just.net
const { setInterval } = just

function createResponses (serverName) {
  // todo: expose this so it can be configured
  const time = (new Date()).toUTCString()
  Object.keys(contentTypes).forEach(contentType => {
    Object.keys(statusMessages).forEach(status => {
      responses[contentType][status] = `HTTP/1.1 ${status} ${statusMessages[status]}\r\nServer: ${serverName}\r\nContent-Type: ${contentTypes[contentType]}\r\nDate: ${time}\r\nContent-Length: `
    })
  })
}

function checkError (fd, event) {
  if (event & EPOLLERR || event & EPOLLHUP) {
    loop.remove(fd)
    net.close(fd)
    return true
  }
  return false
}

function serverOptions (fd, opts = { server: { reuseAddress: true, reusePort: true } }) {
  const { reuseAddress, reusePort } = opts.server
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reuseAddress ? 1 : 0)
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, reusePort ? 1 : 0)
}

function clientOptions (fd, opts = { client: {} }) {
  const { tcpNoDelay, soKeepAlive } = opts.client
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, tcpNoDelay ? 1 : 0)
  setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, soKeepAlive ? 1 : 0)
}

function joinHeaders (headers) {
  return headers.map(h => h.join(': ')).join('\r\n')
}

// TODO: This should really be a Pipeline class with one Response for each one queued in the pipeline
class Response {
  constructor (fd, onFinish) {
    this.fd = fd
    this.pipeline = false
    this.queue = ''
    this.status = 200
    this.headers = []
    this.onFinish = onFinish
  }

  setHeader (...args) {
    // todo: sanitize
    this.headers.push(args)
  }

  end () {
    if (this.headers.length) this.headers = []
    if (this.onFinish) this.onFinish(this.socket.request, this)
    this.status = 200
  }

  json (str) {
    // TODO: we need a buffer for each request and then join them all together or write them out sequentially at the end 
    if (this.pipeline) {
      this.queue += `${json[this.status]}${String.byteLength(str)}${END}${str}`
      return
    }
    sendString(this.fd, `${json[this.status]}${String.byteLength(str)}${END}${str}`)
    this.end()
  }

  // todo: allow calling multiple times and then calling end
  text (str, contentType = text) {
    if (this.pipeline) {
      this.queue += `${contentType[this.status]}${str.length}${END}${str}`
      return
    }
    if (this.headers.length) {
      sendString(this.fd, `${contentType[this.status]}${str.length}${CRLF}${joinHeaders(this.headers)}${END}${str}`)
      return
    }
    sendString(this.fd, `${contentType[this.status]}${str.length}${END}${str}`)
    this.end()
  }

  html (str) {
    if (this.pipeline) {
      this.queue += `${html[this.status]}${str.length}${END}${str}`
      return
    }
    sendString(this.fd, `${html[this.status]}${String.byteLength(str)}${END}${str}`)
    this.end()
  }

  utf8 (str, contentType = utf8) {
    if (this.pipeline) {
      this.queue += `${contentType[this.status]}${String.byteLength(str)}${END}${str}`
      return
    }
    if (this.headers.length) {
      sendString(this.fd, `${contentType[this.status]}${String.byteLength(str)}${CRLF}${joinHeaders(this.headers)}${END}${str}`)
      return
    }
    sendString(this.fd, `${contentType[this.status]}${String.byteLength(str)}${END}${str}`)
    this.end()
  }

  raw (buf, contentType = octet) {
    if (this.headers.length) {
      sendString(this.fd, `${contentType[this.status]}${buf.byteLength}${CRLF}${joinHeaders(this.headers)}${END}`)
      send(this.fd, buf, buf.byteLength, 0)
      return
    }
    sendString(this.fd, `${contentType[this.status]}${buf.byteLength}${END}`)
    send(this.fd, buf, buf.byteLength, 0)
    this.end()
  }

  finish () {
    if (this.pipeline && this.queue.length) {
      // todo: check return codes - backpressure
      sendString(this.fd, this.queue)
      this.queue = ''
    }
  }
}

class Request {
  constructor (fd, index) {
    this.fd = fd
    this.index = index
    this.method = getMethod(index)
    this.url = getUrl(index)
    this.params = []
    this.hasHeaders = false
    this._headers = {}
    this.version = 0
    this.qs = ''
    this.path = ''
    this.query = null
    this.contentLength = 0
    this.bytes = 0
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

  // todo: we need a drop replacement for this that is RFC compliant and safe
  parse (qs = false) {
    if (!qs && this.path) return
    if (qs && this.query) return
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
      if (!this.qs) return
      this.query = this.qs.split('&')
        .map(p => p.split('='))
        .reduce((o, p) => {
          o[p[0]] = p[1]
          return o
        }, {})
    }
  }
}

class Socket {
  constructor (fd, handler, onResponseComplete) {
    // todo - passing hooks in here is kinda ugly
    this.fd = fd
    this.handler = handler
    this.buf = new ArrayBuffer(bufferSize)
    this.len = this.buf.byteLength
    this.off = 0
    // TODO: should we have a response for each request in the pipeline?
    this.response = new Response(fd, onResponseComplete)
    //this.request = new Request(0, 0)
    this.request = null
    this.response.socket = this
    this.inBody = false
    const info = new ArrayBuffer(4)
    this.dv = new DataView(info)
    this.parser = createHandle(this.buf, info)
  }

  close () {
    loop.remove(this.fd)
    close(this.fd)
  }

  onEvent (fd, event) {
    if (checkError(fd, event)) {
      this.close()
      return true
    }
    let bytes = recv(fd, this.buf, this.off, this.len)
    if (bytes <= 0) {
      this.close()
      return true
    }
    if (this.inBody) {
      const { request } = this
      if (request.bytes <= bytes) {
        request.onBody(this.buf, request.bytes, this.off)
        request.inBody = false
        request.onEnd()
        this.off += request.bytes
        bytes -= request.bytes
        request.bytes = 0
      } else {
        request.onBody(this.buf, bytes, this.off)
        this.off = 0
        return false
      }
    }
    const { dv } = this
    // TODO: shouldn't we close here?
    if (bytes === 0) return
    // TODO: we need to loop and keep parsing until all bytes are consumed
    parseRequestsHandle(this.parser, this.off + bytes, 0, answer)
    const r = dv.getUint32(0, true)
    const count = r & 0xff
    const remaining = r >> 16
    if (count < 0) {
      just.error(`parse failed ${count}`)
      this.close()
      return true
    }
    // count will always be 1 if we have a body
    if (count === 1) {
      const request = this.request = new Request(fd, 0)
      this.handler(request, this.response, this)
      if (remaining > 0) {
        if (remaining === bytes) {
          const from = this.off + bytes - remaining
          if (from > 0) {
            just.print(`copyFrom ${remaining} bytes from ${from} to 0`)
            this.buf.copyFrom(this.buf, 0, remaining, from)
          }
          this.off = remaining
          return false
        }
        request.contentLength = parseInt(request.headers[CONTENT_LENGTH] || 0)
        request.onBody(this.buf, remaining, this.off + bytes - remaining)
        request.bytes = request.contentLength - remaining
        if (request.bytes === 0) {
          this.off = 0
          request.onEnd()
          this.inBody = false
        } else {
          this.inBody = true
        }
      } else {
        this.off = 0
        if (request.method === GET) return false
        request.contentLength = parseInt(request.headers[CONTENT_LENGTH] || 0)
        request.bytes = request.contentLength
        if (request.bytes === 0) {
          request.onEnd()
          this.inBody = false
        } else {
          this.inBody = true
        }
      }
      return false
    }
    this.response.pipeline = true
    for (let i = 0; i < count; i++) {
      const request = new Request(fd, i)
      // todo - get return code from handler to decide whether to end now or not
      this.handler(request, this.response, this)
    }
    if (remaining > 0) {
      const from = this.off + bytes - remaining
      if (from > 0) {
        just.print(`copyFrom ${remaining} bytes from ${from} to 0`)
        this.buf.copyFrom(this.buf, 0, remaining, from)
      }
      this.off = remaining
    } else {
      this.off = 0
    }
    this.response.finish()
    return false
  }
}

class Server {
  constructor (opts = { client: {}, server: {} }) {
    this.fd = -1
    this.staticHandlers = {}
    this.regexHandlers = {}
    this.hooks = { pre: [], post: [], connect: [], disconnect: [] }
    this.defaultHandler = this.notFound
    this.name = opts.name || 'just'
    const server = this
    this.timer = setInterval(() => createResponses(server.name), 200)
    this.opts = opts
    this.onError = undefined
    this.sockets = {}
    this.error = 0
    this.address = '127.0.0.1'
    this.port = 3000
    this.stackTraces = false
  }

  connect (handler) {
    this.hooks.connect.push(handler)
    return this
  }

  disconnect (handler) {
    this.hooks.disconnect.push(handler)
    return this
  }

  notFound (req, res) {
    res.status = 404
    res.text(`Not Found ${req.url}`)
  }

  // todo: server.badRequest, server.forbidden, etc.

  serverError (req, res, err) {
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

  match (url, method) {
    for (const handler of this.regexHandlers[method]) {
      const match = url.match(handler.path)
      if (match) {
        return [handler.handler, match.slice(1)]
      }
    }
    return [null, null]
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

  get (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, 'GET')
    return this
  }

  put (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, 'PUT')
    return this
  }

  post (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, 'POST')
    return this
  }

  delete (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, 'DELETE')
    return this
  }

  options (path, handler, opts) {
    if (opts) handler.opts = opts
    this.addPath(path, handler, 'OPTIONS')
    return this
  }

  default (handler, opts) {
    if (opts) handler.opts = opts
    this.defaultHandler = handler
    return this
  }

  close () {
    loop.remove(this.fd)
    net.close(this.fd)
    just.clearInterval(this.timer)
  }

  handleRequest (request, response) {
    const server = this
    if (this.hooks.pre.length) {
      for (const handler of this.hooks.pre) handler(request, response)
    }
    if (response.complete) return
    const methodHandler = this.staticHandlers[request.method]
    if (!methodHandler) {
      this.defaultHandler(request, response)
      return
    }
    let handler = methodHandler[request.url]
    if (handler) {
      if (handler.opts) {
        if (handler.opts.qs) request.parse(true)
        if (handler.opts.async) {
          handler(request, response).catch(err => server.serverError(request, response, err))
          return
        }
        if (handler.opts.err) {
          try {
            handler(request, response)
          } catch (err) {
            this.serverError(request, response, err)
          }
          return
        }
      }
      handler(request, response)
      return
    }
    request.parse()
    handler = methodHandler[request.path]
    if (handler) {
      if (handler.opts) {
        if (handler.opts.qs) request.parse(true)
        if (handler.opts.async) {
          handler(request, response).catch(err => server.serverError(request, response, err))
          return
        }
        if (handler.opts.err) {
          try {
            handler(request, response)
          } catch (err) {
            this.serverError(request, response, err)
          }
          return
        }
      }
      handler(request, response)
      return
    }
    const result = this.match(request.path, request.method)
    if (result[0]) {
      request.params = result[1]
      result[0](request, response)
      return
    }
    handler = this.defaultHandler
    if (handler.opts) {
      if (handler.opts.qs) request.parse(true)
      if (handler.opts.err) {
        try {
          handler(request, response)
        } catch (err) {
          this.serverError(request, response, err)
        }
        return
      }
    }
    handler(request, response)
  }

  use (handler, post = false) {
    if (post) {
      this.hooks.post.push(handler)
      return this
    }
    this.hooks.pre.push(handler)
    return this
  }

  listen (port = 3000, address = '127.0.0.1', maxConn = SOMAXCONN) {
    const fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)
    if (fd < 1) return fd
    this.fd = fd
    serverOptions(fd, this.opts)
    let r = bind(fd, address, port)
    if (r < 0) return r
    r = listen(fd, maxConn)
    if (r < 0) return r
    const server = this
    const { sockets } = server
    const requestHandler = (request, response) => this.handleRequest(request, response)
    function onResponseComplete (request, response) {
      if (server.hooks.post.length) {
        for (const handler of server.hooks.post) handler(request, response)
      }
    }
    loop.add(fd, (fd, event) => {
      if (checkError(fd, event)) return
      const newfd = accept(fd)
      clientOptions(newfd, server.opts)
      let socket
      if (this.hooks.post.length) {
        socket = new Socket(newfd, requestHandler, onResponseComplete)
      } else {
        socket = new Socket(newfd, requestHandler, () => {})
      }
      loop.add(newfd, (fd, event) => {
        if (socket.onEvent(fd, event)) {
          if (this.hooks.disconnect.length) {
            for (const handler of this.hooks.disconnect) handler(socket)
          }
          delete sockets[fd]
        }
      })
      const flags = fcntl(newfd, F_GETFL, 0) | O_NONBLOCK
      fcntl(newfd, F_SETFL, flags)
      loop.update(newfd, EPOLLIN | EPOLLERR | EPOLLHUP)
      this.sockets[newfd] = socket
      if (this.hooks.connect.length) {
        for (const handler of this.hooks.connect) handler(socket)
      }
    })
    this.address = address
    this.port = port
    return this
  }
}

const contentTypes = {
  text: 'text/plain',
  css: 'text/css',
  utf8: 'text/plain; charset=utf-8',
  json: 'application/json; charset=utf-8',
  html: 'text/html; charset=utf-8',
  octet: 'application/octet-stream'
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
  500: 'Server Error'
}
const CONTENT_LENGTH = 'Content-Length'
const GET = 'GET'
const bufferSize = 64 * 1024
const answer = [0, 0]
const responses = { js: {}, text: {}, utf8: {}, json: {}, html: {}, css: {}, octet: {} }
responses.ico = {}
responses.png = {}
responses.xml = {}
contentTypes.ico = 'application/favicon'
contentTypes.png = 'application/png'
contentTypes.xml = 'application/xml; charset=utf-8'
contentTypes.js = 'application/javascript; charset=utf-8'
const END = '\r\n\r\n'
const CRLF = '\r\n'
const PATHSEP = '?'
const { text, utf8, json, html, octet } = responses
const defaultOptions = {
  name: 'just',
  server: {
    reuseAddress: true,
    reusePort: true
  },
  client: {
    tcpNoDelay: false,
    soKeepAlive: false
  }
}

module.exports = {
  createServer: (opts = defaultOptions, handler) => {
    const o = JSON.parse(JSON.stringify(defaultOptions))
    const server = new Server(Object.assign(o, opts))
    if (handler) server.default(handler)
    return server
  },
  defaultOptions,
  responses,
  contentTypes
}
