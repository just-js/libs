const { net } = just.library('net')
const { tls } = just.library('tls', 'openssl.so')
const { epoll } = just.library('epoll')
const socket = require('@socket')

const { AF_INET, SOCK_STREAM, SOCK_NONBLOCK } = net
const { EPOLLIN, EPOLLOUT, EPOLLET } = epoll
const { createSocket, Socket } = socket
const { loop } = just.factory

Socket.prototype.serverName = function () {
  if (!this.tls) return
  return tls.getServerName(this.buffer)
}

Socket.prototype.certificate = function () {
  if (!this.tls) return
  return tls.getCertificate(this.buffer)
}

// required
Socket.prototype.setSecure = function () {
  this.tls = true

  this.recv = function (off = 0, len = this.bufLen - off) {
    const bytes = tls.read(this.buffer, len, off)
    if (bytes > 0) this.stats.recv += bytes
    return bytes
  }
/*
  this.recv = function () {
    const bytes = tls.read(this.buffer)
    if (bytes > 0) this.stats.recv += bytes
    return bytes
  }
*/
  this.send = function (buf, len = buf.byteLength, off = 0) {
    const bytes = tls.write(this.buffer, this.buffer.copyFrom(buf, off, len))
    if (bytes > 0) this.stats.send += bytes
    return bytes
  }
  this.sendString = function (str) {
    const bytes = tls.write(this.buffer, this.buffer.writeString(str))
    if (bytes > 0) this.stats.send += bytes
    return bytes
  }
  return this
}

// https://stackoverflow.com/questions/67657974/c-c-when-and-why-do-we-need-to-call-ssl-do-handshake-in-a-tls-client-server

Socket.prototype.negotiate = function (context = this.context, isClient = this.isClient) {
  if (!this.tls) return Promise.resolve()
  this.context = context
  this.isClient = isClient
  const socket = this
  loop.update(socket.fd, EPOLLOUT | EPOLLIN | EPOLLET)
  //tls.setSNICallback(this.context)          
  return new Promise ((resolve, reject) => {
    if (!this.accepted) {
      let r = 0
      if (socket.isClient) {
        if (context.serverName) {
          r = tls.connectSocket(this.fd, this.context, this.buffer, context.serverName)
        } else {
          r = tls.connectSocket(this.fd, this.context, this.buffer)
        }
      } else {
        r = tls.acceptSocket(this.fd, this.context, this.buffer)
        //const serverName = tls.getServerName(this.buffer)
        //just.print(serverName)
        //if (serverName === 'api.billywhizz.io') {
        //  this.context = createServerContext('cert2.pem', 'key2.pem')
        //  tls.setContext(this.buffer, this.context)
        //}
      }
      if (r === 1) {
        this.accepted = true
        socket.negotiate().then(resolve).catch(reject)
        return
      }
      const err = tls.error(socket.buffer, r)
      if (err === tls.SSL_ERROR_WANT_WRITE) {
        this.accepted = true
        socket.onWritable = () => {
          socket.onWritable = null
          socket.negotiate().then(resolve).catch(reject)
        }
        return
      }
      if (err === tls.SSL_ERROR_WANT_READ) {
        this.accepted = true
        socket.onReadable = () => {
          socket.onReadable = null
          socket.negotiate().then(resolve).catch(reject)
        }
        return
      }
      socket.error = new Error(`TLS Error A ${err}`)
      resolve()
      return
    }
    if (!this.handshake) {
      const r = tls.handshake(socket.buffer)
      if (r === 1) {
        this.handshake = true
        loop.update(socket.fd, socket.events)
        resolve()
        return
      }
      const err = tls.error(socket.buffer, r)
      if (err === tls.SSL_ERROR_WANT_WRITE) {
        socket.onWritable = () => {
          socket.onWritable = null
          socket.negotiate().then(resolve).catch(reject)
        }
        return
      }
      if (err === tls.SSL_ERROR_WANT_READ) {
        socket.onReadable = () => {
          socket.onReadable = null
          socket.negotiate().then(resolve).catch(reject)
        }
        return
      }
      socket.error = new Error(`TLS Error B ${err}`)
      resolve()
      return
    }
  })
}

// required
Socket.prototype.shutdown = function () {
  return tls.shutdown(this.buffer)
}

// required
Socket.prototype.free = function () {
  return tls.free(this.buffer)
}

function createSecureSocket (type = AF_INET, flags = SOCK_STREAM | SOCK_NONBLOCK) {
  return createSocket(type, flags).setSecure()
}

function createServerContext (cert = 'cert.pem', key = 'key.pem') {
  return tls.serverContext(new ArrayBuffer(0), cert, key)
}

function createClientContext (serverName = '', ciphers = '') {
  const context = tls.clientContext(new ArrayBuffer(0))
  if (ciphers) {
    const r = tls.setCiphers(context, ciphers)
    just.print(`setCiphers ${r}`)
  }
  context.serverName = serverName
  return context
}

module.exports = { createSecureSocket, createServerContext, createClientContext }
