const { net } = just.library('net')
const { sys } = just.library('sys')
const { tls } = just.library('tls', 'openssl.so')
const { epoll } = just.library('epoll')
const { http } = just.library('http')

const dns = require('../dns/dns.js')

const {
  getResponses,
  parseResponsesHandle,
  createHandle
} = http

const {
  AF_INET,
  SOCK_STREAM,
  SOCK_NONBLOCK,
  SOL_SOCKET,
  IPPROTO_TCP,
  TCP_NODELAY,
  SO_KEEPALIVE,
  EAGAIN
} = net

const {
  EPOLLERR,
  EPOLLHUP,
  EPOLLIN,
  EPOLLOUT
} = epoll

const { loop } = just.factory

const socketMap = new Map()
const dnsMap = new Map()

function parseUrl (url) {
  const protocolEnd = url.indexOf(':')
  const protocol = url.slice(0, protocolEnd)
  const hostnameEnd = url.indexOf('/', protocolEnd + 3)
  const host = url.slice(protocolEnd + 3, hostnameEnd)
  const path = url.slice(hostnameEnd)
  const [hostname, port] = host.split(':')
  if (port) return { protocol, host, hostname, path, port }
  return { protocol, host, hostname, path, port: protocol === 'https' ? 443 : 80 }
}

function getIPAddress (hostname, callback) {
  return new Promise((resolve, reject) => {
    if (dnsMap.has(hostname)) {
      resolve(dnsMap.get(hostname))
      return
    }
    dns.lookup(hostname, (err, ip) => {
      if (err) {
        reject(err)
        return
      }
      dnsMap.set(hostname, ip)
      resolve(ip)
    })
  })
}

function acquireSocket (ip, port) {
  const key = `${ip}:${port}`
  if (socketMap.has(key)) {
    const sockets = socketMap.get(key)
    if (sockets.length) {
      const sock = sockets.shift()
      sock.free = false
      return sock
    }
  }
  const fd = net.socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)
  return new Socket(fd, ip, port)
}

function releaseSocket (sock) {
  if (sock.free) return
  const key = `${sock.ip}:${sock.port}`
  sock.free = true
  if (socketMap.has(key)) {
    socketMap.get(key).push(sock)
    return
  }
  socketMap.set(`${sock.ip}:${sock.port}`, [sock])
}

function writeRequest (sock, req) {
  const headers = `${req.method} ${req.path} HTTP/1.1\r\nUser-Agent: ${req.userAgent}\r\nAccept: ${req.accept}\r\nHost: ${req.hostname}\r\n\r\n`
  const bytes = sock.write(headers)
  if (bytes < headers.length) {
    just.print('short write')
  }
  return bytes
}

function getResponse () {
  const responses = [[]]
  getResponses(1, responses)
  return responses.map(res => {
    const [version, statusCode, statusMessage, headers] = res
    res.chunked = false
    return { version, statusCode, statusMessage, headers }
  })[0]
}

class Request {
  constructor (url, options = { method: 'GET' }) {
    this.url = url
    const { protocol, hostname, path, port } = parseUrl(url)
    this.protocol = protocol
    this.hostname = hostname
    this.port = port
    this.path = path
    this.method = options.method || 'GET'
    this.fileName = options.fileName
    this.headers = options.headers
    this.userAgent = 'curl/7.58.0'
    this.accept = '*/*'
  }
}

class Socket {
  constructor (fd, ip, port, options = { buffer: new ArrayBuffer(16384), keepAlive: true, noDelay: true }) {
    this.fd = fd
    this.ip = ip
    this.port = port
    this.closed = false
    this.tls = null
    this.paused = false
    this.secure = false
    this.connected = false
    this.keepAlive = options.keepAlive || false
    this.noDelay = options.noDelay || false
    this.buffer = options.buffer || new ArrayBuffer(16384)
    this.free = false
    this.buffer.offset = 0
  }

  #onEvent (event) {
    if (event & EPOLLIN) this.onReadable()
    if (event & EPOLLOUT) this.onWritable()
  }

  onReadable () {}
  onWritable () {}

  pull () {
    const socket = this
    return new Promise((resolve, reject) => {
      function next () {
        socket.onReadable = () => {
          socket.onReadable = () => {}
          const bytes = socket.read()
          if (bytes >= 0) {
            resolve(bytes)
            return
          }
          next()
        }
      }
      next()
    })
  }

  read () {
    // todo: non-secure
    const { offset, byteLength } = this.buffer
    const expected = byteLength - offset 
    const bytes = tls.read(this.buffer, expected, offset)
    if (bytes === 0) {
      const err = tls.error(this.buffer, bytes)
      if (err === tls.SSL_ERROR_ZERO_RETURN) {
        just.print('tls read error: ssl has been shut down')
      } else {
        just.print('tls read error: connection has been aborted')
      }
      this.close()
      return bytes
    }
    if (bytes < 0) {
      const err = tls.error(this.buffer, bytes)
      if (err === tls.SSL_ERROR_WANT_READ) {
        const errno = sys.errno()
        if (errno !== EAGAIN) {
          just.print(`tls read error: ${sys.errno()}: ${sys.strerror(sys.errno())}`)
          this.close()
        }
      } else {
        just.print(`tls error ${err}: ${sys.errno()}: ${sys.strerror(sys.errno())}`)
        this.close()
      }
    }
    return bytes
  }

  write (str) {
    if (this.secure) {
      return tls.write(this.buffer, this.buffer.writeString(str))
    }
    return net.writeString(this.fd, str)
  }

  pause () {
    loop.update(this.fd, EPOLLOUT)
    this.paused = true
  }

  resume () {
    loop.update(this.fd, EPOLLIN | EPOLLOUT)
    this.paused = false
  }

  setNoDelay (on = true) {
    net.setsockopt(this.fd, IPPROTO_TCP, TCP_NODELAY, on ? 1 : 0)
  }

  setKeepalive (on = true) {
    net.setsockopt(this.fd, SOL_SOCKET, SO_KEEPALIVE, on ? 1 : 0)
  }

  handshake () {
    if (this.secure) return Promise.resolve()
    const socket = this
    this.tls = tls.clientContext(new ArrayBuffer(0))
    return new Promise((resolve, reject) => {
      loop.add(this.fd, (fd, event) => {
        if (event & EPOLLERR || event & EPOLLHUP) {
          this.close()
          return
        }
        let r = 0
        if (!socket.connected) {
          r = tls.connectSocket(socket.fd, socket.tls, socket.buffer)
          socket.connected = true
        } else if (!socket.secure) {
          r = tls.handshake(socket.buffer)
        }
        if (r === 1) {
          socket.secure = true
          loop.handles[socket.fd] = (fd, event) => socket.#onEvent(event)
          socket.resume()
          resolve()
          return
        }
        const err = tls.error(socket.buffer, r)
        if (err === tls.SSL_ERROR_WANT_WRITE) {
          loop.update(socket.fd, EPOLLOUT)
        } else if (err === tls.SSL_ERROR_WANT_READ) {
          loop.update(socket.fd, EPOLLIN)
        } else {
          reject(new Error('socket handshake error'))
        }
      }, EPOLLOUT)
    })
  }

  connect () {
    if (this.connected && !this.closed) return Promise.resolve()
    const socket = this
    const { ip, port } = socket
    const ok = net.connect(socket.fd, ip, port)
    if (ok < 0) {
      if (sys.errno() !== 115) throw new just.SystemError('net.connect')
    }
    if (this.keepAlive) this.setKeepalive(true)
    if (this.noDelay) this.setNoDelay(true)
    return new Promise((resolve, reject) => {
      loop.add(socket.fd, (fd, event) => {
        if (event & EPOLLERR || event & EPOLLHUP) {
          socket.close()
          reject(new just.SystemError('epoll.error'))
          return
        }
        if (event & EPOLLOUT) {
          loop.remove(socket.fd)
          resolve()
          return
        }
        reject(new Error('unexpected epoll state'))
      }, EPOLLOUT)
    })
  }

  release () {
    releaseSocket(this)
  }

  close () {
    if (this.closed) return
    loop.remove(this.fd)
    if (this.tls) {
      if (this.secure) tls.shutdown(this.buffer)
      tls.free(this.buffer)
    }
    net.close(this.fd)
    this.closed = true
    socketMap.delete(`${this.ip}:${this.port}`)
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
            just.print('empty return')
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
        //just.print(`len ${len} remaining ${remaining} size ${this.size}`)
        if (remaining > len) {
          chunks.push(this.buffer.slice(off, off + len))
          this.consumed += len
          off += len
          len = 0
        } else {
          chunks.push(this.buffer.slice(off, off + remaining))
          len -= remaining
          off += remaining
          this.reset()
        }
      }
    }
    return chunks
  }
}

async function fetch (url, options) {
  const req = new Request(url, options)
  const ip = await getIPAddress(req.hostname)
  const sock = acquireSocket(ip, req.port)
  await sock.connect()

  if (req.protocol === 'https') {
    await sock.handshake()
  }

  writeRequest(sock, req)

  const info = new ArrayBuffer(4)
  const dv = new DataView(info)
  const handle = createHandle(sock.buffer, info)
  let body = []
  let bodyLen = 0
  let remaining = 0

  let bytes = await sock.pull()
  while (bytes) {
    parseResponsesHandle(handle, sock.buffer.offset + bytes, 0)
    const r = dv.getUint32(0, true)
    const count = r & 0xff
    remaining = r >> 16
    if (count > 0) break
    sock.buffer.offset = remaining
    bytes = await sock.pull()
  }

  const res = getResponse()
  res.json = async () => {
    const text = await res.text()
    return JSON.parse(text)
  }
  res.pull = async () => {
    if (res.contentLength && (bodyLen === res.contentLength)) return
    if (body.length) {
      return body.shift()
    }
    let bytes = await sock.pull()
    if (bytes < 0) throw new SystemError('pull')
    if (bytes === 0) return
    if (res.chunked) {
      just.print(bytes)
      const chunks = parser.parse(bytes, 0)
      if (!chunks) return
      just.print(chunks.length)
      if (chunks.length) body = body.concat(chunks)
      return body.shift()
    }
    bodyLen += bytes
    return sock.buffer.slice(0, bytes)
  }
  res.chunkedText = async () => {
    let bytes = await sock.pull()
    while (bytes) {
      const chunks = parser.parse(bytes, 0)
      if (!chunks) break
      if (chunks.length) body = body.concat(chunks)
      bodyLen += bytes
      bytes = await sock.pull()
    }
    releaseSocket(sock)
    return body.map(buf => buf.readString(buf.byteLength, 0)).join('')
  }
  res.text = async () => {
    if (res.chunked) return res.chunkedText()
    if (res.contentLength && (bodyLen === res.contentLength)) {
      releaseSocket(sock)
      return body.map(buf => buf.readString(buf.byteLength, 0)).join('')
    }
    let bytes = await sock.pull()
    while (bytes) {
      body.push(sock.buffer.slice(0, bytes))
      bodyLen += bytes
      if (res.contentLength && (bodyLen === res.contentLength)) {
        releaseSocket(sock)
        return body.map(buf => buf.readString(buf.byteLength, 0)).join('')
      }
      bytes = await sock.pull()
    }
  }
  res.request = req
  res.sock = sock
  let parser

  sock.buffer.offset = 0
  if (res.headers['Content-Length']) {
    res.contentLength = parseInt(res.headers['Content-Length'], 10)
  } else {
    res.contentLength = 0
    if (res.headers['Transfer-Encoding'] === 'chunked') {
      res.chunked = true
      parser = new ChunkParser(sock.buffer)
    }
  }
  if (remaining > 0) {
    if (res.chunked) {
      const chunks = parser.parse(remaining, sock.buffer.offset + bytes - remaining)
      if (chunks.length) body = body.concat(chunks)
    } else {
      body.push(sock.buffer.slice(sock.buffer.offset + bytes - remaining, sock.buffer.offset + bytes))
      bodyLen += remaining
    }
  }
  if (!res.chunked && (bodyLen === res.contentLength)) {
    releaseSocket(sock)
  }

  return res
}

module.exports = { fetch }
