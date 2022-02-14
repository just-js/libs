const { net } = just.library('net')
const { epoll } = just.library('epoll')
const { sys } = just.library('sys')

const {
  AF_INET,
  AF_UNIX,
  SOCK_STREAM,
  SOCK_NONBLOCK,
  SOL_SOCKET,
  IPPROTO_TCP,
  TCP_NODELAY,
  SO_KEEPALIVE,
  SO_REUSEADDR,
  SO_REUSEPORT,
  EAGAIN,
  O_NONBLOCK
} = net

const {
  EPOLLERR,
  EPOLLHUP,
  EPOLLIN,
  EPOLLOUT,
  EPOLLET
} = epoll

const { errno, fcntl, F_GETFL, F_SETFL } = sys

const { loop } = just.factory
const { SystemError } = just

class SocketStats {
  constructor () {
    this.send = 0
    this.recv = 0
  }
}

class Socket {
  constructor (fd) {
    this.fd = fd
    this.connected = false
    this.closed = false
    this.buffer = new ArrayBuffer(65536)
    this.bufLen = this.buffer.byteLength
    this.error = null
    this.stats = new SocketStats()
    this.events = EPOLLIN | EPOLLET
    this.flags = 0
    this.maxConn = 4096
    this.readable = false
    this.writable = false
    // TLS
    this.handshake = false
    this.accepted = false
    this.context = null
    this.isClient = false
    this.tls = false
  }

  //get serverName () {}
  get certificate () {}

  set edgeTriggered (on = true) {
    if (on && (this.events & EPOLLET === 0)) {
      this.events = EPOLLIN | EPOLLET
      return loop.update(this.fd, this.events)
    } else if (!on && ((this.events & EPOLLET === EPOLLET))) {
      this.events = EPOLLIN
      return loop.update(this.fd, this.events)
    }
  }

  set nonBlocking (on = true) {
    let flags = fcntl(this.fd, F_GETFL, 0)
    if (flags < 0) return flags
    if (on) {
      flags = flags | O_NONBLOCK
    } else {
      flags = flags & ~O_NONBLOCK
    }
    this.flags = flags
    fcntl(this.fd, F_SETFL, this.flags)
  }

  setSecure () {}

  recv (off = 0, len = this.bufLen - off) {
    const bytes = net.recv(this.fd, this.buffer, off, len)
    if (bytes > 0) this.stats.recv += bytes
    return bytes
  }

  send (buf, len = buf.byteLength, off = 0) {
    const bytes = net.send(this.fd, buf, len, off)
    if (bytes > 0) this.stats.send += bytes
    return bytes
  }

  sendString (str) {
    const bytes = net.sendString(this.fd, str)
    if (bytes > 0) this.stats.send += bytes
    return bytes
  }

  close () {
    if (this.closed) return
    if (this.tls) this.shutdown()
    this.closed = true
    if (this.onReadable) this.onReadable()
    loop.remove(this.fd)
    this.onClose()
    if (this.tls) {
      this.free()
      this.context = null
    }
    return net.close(this.fd)
  }

  listen () {
    const socket = this
    const r = loop.add(this.fd, (fd, event) => {
      if (event & EPOLLERR || event & EPOLLHUP) {
        // TODO
        just.print('accept error')
        return
      }
      if (socket.onConnection) socket.onConnection()
    }, EPOLLIN | EPOLLET)
    if (r < 0) return r
    return net.listen(socket.fd, socket.maxConn)
  }

  pause () {
    this.paused = true
    return loop.update(this.fd, EPOLLOUT)
  }

  resume () {
    this.paused = false
    return loop.update(this.fd, this.events)
  }

  get blocked () {
    this.error = null
    const err = errno()
    if (err === EAGAIN) return false
    this.error = new SystemError('socket.error')
    return true
  }

  negotiate (context = this.context, isClient = this.isClient) {}

  pull (off = 0, len = this.bufLen - off) {
    const socket = this
    const bytes = socket.recv(off, len)
    if (bytes >= 0) return Promise.resolve(bytes)
    const err = errno()
    if (err === EAGAIN) {
      socket.readable = false
      return new Promise((resolve, reject) => {
        // todo: this will clobber any previous ones
        socket.onReadable = () => {
          socket.onReadable = null
          socket.readable = true
          socket.pull(off, len).then(resolve).catch(reject)
        }
      })
    }
    socket.error = new SystemError(`socket.recv (${socket.fd})`)
    return Promise.resolve(0)
  }

  push (buf, len = buf.byteLength, off = 0) {
    const socket = this
    const bytes = socket.send(buf, len, off)
    if (bytes === len) return Promise.resolve(bytes)
    if (bytes > 0 || errno() === EAGAIN) {
      socket.writable = false
      return new Promise((resolve, reject) => {
        // todo: this will clobber any previous ones
        socket.onWritable = () => {
          socket.onWritable = null
          sock.writable = true
          if (bytes > 0) {
            socket.push(buf, len - bytes, bytes).then(resolve).catch(reject)
            return
          }
          socket.push(buf, len).then(resolve).catch(reject)
        }
      })
    }
    socket.error = new SystemError(`socket.send (${socket.fd})`)
    return Promise.resolve(0)
  }

  pushString (str, len = String.byteLength(str)) {
    const socket = this
    const bytes = socket.sendString(str)
    if (bytes === len) return bytes
    if (bytes > 0 || errno() === EAGAIN) {
      socket.writable = false
      return new Promise((resolve, reject) => {
        socket.onWritable = () => {
          socket.onWritable = null
          sock.writable = true
          socket.pushString(str, len).then(resolve).catch(reject)
        }
      })
    }
    socket.error = new SystemError(`socket.send (${socket.fd})`)
    return Promise.resolve(0)
  }

  accept () {
    const socket = this
    return new Promise((resolve, reject) => {
      const fd = net.accept(this.fd)
      if (fd > 0) {
        const sock = new socket.constructor(fd)
        if (socket.tls) sock.setSecure()
        sock.nonBlocking = true
        loop.add(fd, (fd, event) => sock.onSocketEvent(event), socket.events)
        resolve(sock)
        return
      }
      socket.onConnection = () => {
        socket.onConnection = null
        socket.accept().then(resolve).catch(reject)
      }
    })
  }

  onSocketEvent (event) {
    const socket = this
    if (event & EPOLLERR || event & EPOLLHUP) {
      socket.error = new SystemError(`socket.error (${socket.fd})`)
      socket.close()
      return
    }
    if (event & EPOLLIN && socket.onReadable) socket.onReadable()
    if (event & EPOLLOUT && socket.onWritable) socket.onWritable()
  }

  onConnectEvent (event, resolve, reject) {
    const socket = this
    if (event & EPOLLERR || event & EPOLLHUP) {
      reject(new SystemError('connect'))
      socket.close()
      return
    }
    if (!socket.connected) {
      socket.connected = true
      loop.handles[socket.fd] = (fd, event) => socket.onSocketEvent(event)
      loop.update(socket.fd, socket.events)
      resolve(socket)
    }
  }

  onConnection () {}
  onClose () {}
}

class UnixSocket extends Socket {
  constructor (fd) {
    super(fd)
  }

  connect (path = 'unix.sock') {
    const socket = this
    return new Promise((resolve, reject) => {
      const r = net.connect(this.fd, path)
      if (r < 0 && errno() !== 115) {
        reject(new SystemError('connect'))
        return
      }
      loop.add(socket.fd, (fd, event) => {
        socket.onConnectEvent(event, resolve, reject)
      }, EPOLLIN | EPOLLOUT | EPOLLET)
    })
  }

  listen (path = 'unix.sock') {
    const r = net.bind(this.fd, path)
    if (r < 0) return r
    return super.listen()
  }
}

class TCPSocket extends Socket {
  constructor (fd) {
    super(fd)
    this.reusePort = true
    this.reuseAddress = true
  }

  set noDelay (on = true) {
    return net.setsockopt(this.fd, IPPROTO_TCP, TCP_NODELAY, on ? 1 : 0)
  }

  set keepAlive (on = true) {
    return net.setsockopt(this.fd, SOL_SOCKET, SO_KEEPALIVE, on ? 1 : 0)
  }

  set reusePort (on = true) {
    return net.setsockopt(this.fd, SOL_SOCKET, SO_REUSEPORT, on ? 1 : 0)
  }

  set reuseAddress (on = true) {
    return net.setsockopt(this.fd, SOL_SOCKET, SO_REUSEADDR, on ? 1 : 0)
  }

  connect (address = '127.0.0.1', port = 3000) {
    const socket = this
    return new Promise((resolve, reject) => {
      const r = net.connect(this.fd, address, port)
      if (r < 0 && errno() !== 115) {
        reject(new SystemError('connect'))
        return
      }
      loop.add(socket.fd, (fd, event) => {
        socket.onConnectEvent(event, resolve, reject)
      }, EPOLLIN | EPOLLOUT | EPOLLET)
    })
  }

  listen (address = '127.0.0.1', port = 3000) {
    const r = net.bind(this.fd, address, port)
    if (r < 0) return r
    return super.listen()
  }
}

const socketTypes = {
  tcp: AF_INET,
  unix: AF_UNIX
}

function createSocket (type = AF_INET, flags = SOCK_STREAM | SOCK_NONBLOCK) {
  const fd = net.socket(type, flags, 0)
  // todo: handle error
  if (type === AF_INET) return new TCPSocket(fd)
  return new UnixSocket(fd)
}

module.exports = { createSocket, Socket, TCPSocket, UnixSocket, socketTypes }
