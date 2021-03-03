const { epoll } = just.library('epoll')
const { http } = just.library('http')
const { sys } = just.library('sys')
const { net } = just.library('net')

const { parseRequests, getUrl, getMethod, getHeaders } = http
const { EPOLLIN, EPOLLERR, EPOLLHUP } = epoll
const { close, recv, accept, setsockopt, socket, bind, listen, sendString } = net
const { fcntl } = sys
const { loop } = just.factory
const { F_GETFL, F_SETFL } = just.sys
const { IPPROTO_TCP, O_NONBLOCK, TCP_NODELAY, SO_KEEPALIVE, SOMAXCONN, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, SO_REUSEPORT, SOCK_NONBLOCK } = just.net
const { setInterval } = just

const contentTypes = {
  text: 'text/plain',
  utf8: 'text/plain; charset=utf-8',
  json: 'application/json; charset=utf-8',
  html: 'text/html; charset=utf-8'
}

const statusMessages = {
  200: 'OK',
  201: 'Created',
  204: 'OK',
  400: 'Bad Request',
  404: 'Not Found',
  500: 'Server Error'
}

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
    if (this.onFinish) this.onFinish(this)
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

  text (str) {
    if (this.pipeline) {
      this.queue += `${text[this.status]}${str.length}${END}${str}`
      return
    }
    //if (this.headers.length) {
    //  sendString(this.fd, `${text[this.status]}${str.length}${CRLF}${joinHeaders(this.headers)}${END}${str}`)
    //  return
    //}
    sendString(this.fd, `${text[this.status]}${str.length}${END}${str}`)
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

  utf8 (str) {
    if (this.pipeline) {
      this.queue += `${utf8[this.status]}${String.byteLength(str)}${END}${str}`
      return
    }
    sendString(this.fd, `${utf8[this.status]}${String.byteLength(str)}${END}${str}`)
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
  }

  get headers () {
    if (this.hasHeaders) return this._headers
    this.version = getHeaders(this.index, this._headers)
    this.hasHeaders = true
    return this._headers
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
    const bytes = recv(fd, this.buf, this.off, this.len)
    if (bytes <= 0) {
      this.close()
      return true
    }
    const [remaining, count] = parseRequests(this.buf, this.off + bytes, 0, answer)
    if (count < 0) {
      just.error(`parse failed ${count}`)
      this.close()
      return true
    }
    this.response.pipeline = (count > 1)
    for (let i = 0; i < count; i++) {
      const request = new Request(fd, i)
      // todo - get return code from handler to decide whether to end now or not
      this.handler(request, this.response)
    }
    this.response.finish()
    if (remaining > 0) {
      const from = this.off + bytes - remaining
      if (from > 0) {
        just.print(`copyFrom ${remaining} bytes from ${from} to 0`)
        this.buf.copyFrom(this.buf, 0, remaining, from)
      }
      this.off = remaining
      return false
    }
    this.off = 0
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

  serverError (req, res, err) {
    res.status = 500
    res.text(err.stack)
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
    if (typeof path === 'string') {
      this.staticHandlers[method][path] = handler
      return
    }
    if (typeof path === 'object') {
      if (path.constructor.name === 'RegExp') {
        this.regexHandlers[method].push({ handler, path })
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
    if (this.hooks.pre.length) {
      for (const handler of this.hooks.pre) handler(request, response)
    }
    const methodHandler = this.staticHandlers[request.method]
    if (!methodHandler) {
      this.defaultHandler(request, response)
      return
    }
    let handler = methodHandler[request.url]
    if (handler) {
      if (handler.opts) {
        if (handler.opts.qs) request.parse(true)
      }
      handler(request, response)
      return
    }
    request.parse()
    handler = methodHandler[request.path]
    if (handler) {
      if (handler.opts) {
        if (handler.opts.qs) request.parse(true)
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
    if (this.defaultHandler.opts) {
      if (this.defaultHandler.opts.qs) request.parse(true)
    }
    this.defaultHandler(request, response)
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
    bind(fd, address, port)
    this.error = listen(fd, maxConn)
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
      clientOptions(newfd, this.opts)
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

const bufferSize = 64 * 1024
const answer = [0, 0]
const responses = { text: {}, utf8: {}, json: {}, html: {} }
const END = '\r\n\r\n'
const CRLF = '\r\n'
const PATHSEP = '?'
const { text, utf8, json, html } = responses
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
  createServer: (handler, opts = defaultOptions) => {
    const server = new Server(opts)
    if (handler) server.default(handler)
    return server
  },
  defaultOptions
}
