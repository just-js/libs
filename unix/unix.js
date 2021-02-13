const { sys, net } = just
const { EPOLLIN, EPOLLERR, EPOLLHUP, EPOLLOUT } = just.loop
const { O_NONBLOCK, SOMAXCONN, SOCK_STREAM, SOL_SOCKET, AF_UNIX, SOCK_NONBLOCK, SO_ERROR } = net
const { loop } = just.factory
const { unlink } = just.fs

const readableMask = EPOLLIN | EPOLLERR | EPOLLHUP
const readableWritableMask = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLOUT

function createServer (path) {
  const server = { path }
  const sockets = {}

  function closeSocket (sock) {
    if (sock.closing) return
    const { fd } = sock
    sock.closing = true
    sock.onClose && sock.onClose(sock)
    delete sockets[fd]
    loop.remove(fd)
    net.close(fd)
  }

  function onConnect (fd, event) {
    if (event & EPOLLERR || event & EPOLLHUP) {
      return closeSocket({ fd })
    }
    const clientfd = net.accept(fd)
    const socket = sockets[clientfd] = { fd: clientfd }
    loop.add(clientfd, (fd, event) => {
      if (event & EPOLLERR || event & EPOLLHUP) {
        closeSocket(socket)
        return
      }
      const { offset } = buffer
      // TODO: it would be better if we raised a readable even and the client 
      // did the reading and handled any errors. otherwise the error becomes 
      // disassociated from the read and has to be sent in an anonymous 
      // onClose/onError callback
      const bytes = net.recv(fd, buffer, offset, byteLength - offset)
      if (bytes > 0) {
        socket.onData(bytes)
        return
      }
      if (bytes < 0) {
        const errno = sys.errno()
        if (errno === net.EAGAIN) return
        just.error(`recv error: ${sys.strerror(errno)} (${errno})`)
      }
      closeSocket(socket)
    })
    let flags = sys.fcntl(clientfd, sys.F_GETFL, 0)
    flags |= O_NONBLOCK
    sys.fcntl(clientfd, sys.F_SETFL, flags)
    loop.update(clientfd, readableMask)
    socket.write = (buf, len = byteLength, off = 0) => {
      const written = net.send(clientfd, buf, len, off)
      if (written > 0) {
        return written
      }
      if (written < 0) {
        const errno = sys.errno()
        if (errno === net.EAGAIN) return written
        just.error(`write error (${clientfd}): ${sys.strerror(errno)} (${errno})`)
      }
      if (written === 0) {
        just.error(`zero write ${clientfd}`)
      }
      return written
    }
    socket.writeString = str => net.sendString(clientfd, str)
    socket.close = () => closeSocket(socket)
    const buffer = server.onConnect(socket)
    const byteLength = buffer.byteLength
    buffer.offset = 0
  }

  function listen (maxconn = SOMAXCONN) {
    const r = net.listen(sockfd, maxconn)
    if (r === 0) loop.add(sockfd, onConnect)
    return r
  }
  server.listen = listen
  server.close = () => net.close(sockfd)
  server.bind = () => net.bind(sockfd, path)
  const sockfd = net.socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0)
  unlink(path)
  net.bind(sockfd, path)
  return server
}

function createClient (path) {
  const sock = { path, connected: false }
  let fd
  let byteLength = 0

  function closeSocket () {
    if (sock.closing) return
    sock.closing = true
    sock.onClose && sock.onClose(sock)
    loop.remove(fd)
    net.close(fd)
  }

  function handleRead (fd, event) {
    const { offset } = buffer
    const bytes = net.recv(fd, buffer, offset, byteLength - offset)
    if (bytes > 0) {
      sock.onData(bytes)
      return
    }
    if (bytes < 0) {
      const errno = sys.errno()
      if (errno === net.EAGAIN) return
      just.print(`recv error: ${sys.strerror(errno)} (${errno})`)
    }
    closeSocket(sock)
  }

  function handleError (fd, event) {
    const errno = net.getsockopt(fd, SOL_SOCKET, SO_ERROR)
    if (!sock.connected) {
      sock.onConnect(new Error(`${errno} : ${just.sys.strerror(errno)}`))
    }
  }

  function handleWrite (fd, event) {
    if (!sock.connected) {
      let flags = sys.fcntl(fd, sys.F_GETFL, 0)
      flags |= O_NONBLOCK
      sys.fcntl(fd, sys.F_SETFL, flags)
      loop.update(fd, readableMask)
      buffer = sock.onConnect(null, sock)
      byteLength = buffer.byteLength
      buffer.offset = 0
      sock.connected = true
    }
  }

  function onSocketEvent (fd, event) {
    if (event & EPOLLERR || event & EPOLLHUP) {
      handleError(fd, event)
      closeSocket()
      return
    }
    if (event & EPOLLIN) {
      handleRead(fd, event)
    }
    if (event & EPOLLOUT) {
      handleWrite(fd, event)
    }
  }

  sock.write = (buf, len = buf.byteLength, off = 0) => {
    const written = net.send(fd, buf, len, off)
    if (written > 0) {
      return written
    }
    if (written < 0) {
      const errno = sys.errno()
      if (errno === net.EAGAIN) return written
      just.error(`write error (${fd}): ${sys.strerror(errno)} (${errno})`)
    }
    if (written === 0) {
      just.error(`zero write ${fd}`)
    }
    return written
  }
  sock.writeString = str => net.sendString(fd, str)

  sock.close = () => closeSocket(sock)

  function connect () {
    fd = net.socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0)
    loop.add(fd, onSocketEvent, readableWritableMask)
    net.connect(fd, path)
    sock.fd = fd
    return sock
  }

  let buffer
  sock.connect = connect
  return sock
}

module.exports = { createServer, createClient }
