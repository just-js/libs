const { fs } = just.library('fs')
const { net } = just.library('net')

const { SystemError, clearTimeout, setInterval } = just

class Monitor {
  constructor (stats = [], opts = { bufferSize: 16384, interval: 100 }) {
    this.stats = stats
    this.bufferSize = opts.bufferSize || 16384
    this.buffer = new ArrayBuffer(this.bufferSize)
    this.interval = opts.interval || 100
    this.timer = 0
    this.pid = 0
    this.fd = 0
    this.path = ''
  }

  read () {
    const { stats, pid, buffer, fd, bufferSize } = this
    net.seek(fd, 0, net.SEEK_SET)
    // todo: stat the file and create a buffer of correct size
    let bytes = net.read(fd, buffer, 0, bufferSize)
    const parts = []
    while (bytes > 0) {
      parts.push(buffer.readString(bytes))
      bytes = net.read(fd, buffer, 0, bufferSize)
    }
    const fields = parts.join('').split(' ')
    const comm = fields[1]
    const state = fields[2]
    const [
      ppid,
      pgrp,
      session,
      ttyNr,
      tpgid,
      flags,
      minflt,
      cminflt,
      majflt,
      cmajflt,
      utime,
      stime,
      cutime,
      cstime,
      priority,
      nice,
      numThreads,
      itrealvalue,
      starttime,
      vsize,
      rssPages,
      rsslim,
      startcode,
      endcode,
      startstack,
      kstkesp,
      kstkeip,
      signal,
      blocked,
      sigignore,
      sigcatch,
      wchan,
      nswap,
      cnswap,
      exitSignal,
      processor,
      rtPriority,
      policy,
      delayacctBlkioTicks,
      guestTime,
      cguestTime,
      startData,
      endData,
      startBrk,
      argStart,
      argEnd,
      envStart,
      envEnd,
      exitCode
    ] = fields.slice(3).map(v => Number(v))
    stats.push({
      pid,
      comm,
      state,
      ppid,
      pgrp,
      session,
      ttyNr,
      tpgid,
      flags,
      minflt,
      cminflt,
      majflt,
      cmajflt,
      utime,
      stime,
      cutime,
      cstime,
      priority,
      nice,
      numThreads,
      itrealvalue,
      starttime,
      vsize,
      rssPages,
      rsslim,
      startcode,
      endcode,
      startstack,
      kstkesp,
      kstkeip,
      signal,
      blocked,
      sigignore,
      sigcatch,
      wchan,
      nswap,
      cnswap,
      exitSignal,
      processor,
      rtPriority,
      policy,
      delayacctBlkioTicks,
      guestTime,
      cguestTime,
      startData,
      endData,
      startBrk,
      argStart,
      argEnd,
      envStart,
      envEnd,
      exitCode
    })
  }

  open (pid) {
    this.close()
    this.path = `/proc/${pid}/stat`
    this.fd = fs.open(this.path)
    if (this.fd <= 0) throw new SystemError(`could not open ${this.path}`)
    this.timer = setInterval(() => this.read(), this.interval)
  }

  close () {
    if (this.fd) {
      net.close(this.fd)
      this.fd = 0
      clearTimeout(this.timer)
    }
    this.pid = 0
  }
}

module.exports = { Monitor }
