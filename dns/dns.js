const { create, parse } = require('protocol.js')
const { udp } = just.library('udp')
const { net } = just
const { loop } = just.factory
const { readFile, isFile } = require('fs')

const dnsServer = just.env().DNS_SERVER || '127.0.0.11'

function parseLine (line) {
  const parts = line.split(/\s+/)
  const [address, ...hosts] = parts
  return { address, hosts }
}

const rxipv4 = /\d+\.\d+\.\d+\.\d+/
const rxComment = /(\s+)?#.+/
const rxName = /nameserver\s+(.+)/

function readHosts () {
  const ipv4 = {}
  const ipv6 = {}
  const fileName = '/etc/hosts'
  if (!isFile(fileName)) {
    just.error(`${fileName} not found`)
    return { ipv4, ipv6 }
  }
  const hosts = readFile(fileName)
  const lines = hosts.split('\n').filter(line => line.trim())
  for (const line of lines) {
    if (line.match(rxComment)) continue
    const { address, hosts } = parseLine(line)
    if (address.match(rxipv4)) {
      for (const host of hosts) {
        ipv4[host] = address
      }
    } else {
      for (const host of hosts) {
        ipv6[host] = address
      }
    }
  }
  return { ipv4, ipv6 }
}

function lookupHosts (hostname) {
  const { ipv4 } = readHosts()
  return ipv4[hostname]
}

function readResolv () {
  const fileName = '/etc/resolv.conf'
  const results = []
  if (!isFile(fileName)) {
    just.error(`${fileName} not found`)
    return results
  }
  const resolv = readFile(fileName)
  const lines = resolv.split('\n').filter(line => line.trim())
  for (const line of lines) {
    const match = line.match(rxName)
    if (match && match.length > 1) {
      const [, ip] = match
      if (ip.match(rxipv4)) {
        results.push(ip)
      }
    }
  }
  return results
}

function lookup (query = 'www.google.com', onRecord = () => {}, address = dnsServer, port = 53, buf = new ArrayBuffer(65536)) {
  const ip = lookupHosts(query)
  const byteLength = buf.byteLength
  if (ip) {
    onRecord(null, ip)
    return
  }
  const ips = readResolv()
  if (ips.length) {
    address = ips[0]
  }
  const fd = net.socket(net.AF_INET, net.SOCK_DGRAM | net.SOCK_NONBLOCK, 0)
  net.bind(fd, address, port)
  loop.add(fd, (fd, event) => {
    just.clearTimeout(timer)
    const answer = []
    const len = udp.recvmsg(fd, buf, answer, byteLength)
    if (len <= 0) {
      onRecord(new Error('Bad Message Length'))
      return
    }
    const message = parse(buf, len)
    if (!message.answer.length) {
      onRecord(new Error(`Address Not Found for ${query}`))
      return
    }
    if (message.answer.length === 0 && message.answer[0].ctype === 1) {
      const { ip } = message.answer[0]
      const result = `${ip[0]}.${ip[1]}.${ip[2]}.${ip[3]}`
      loop.remove(fd)
      net.close(fd)
      onRecord(null, result)
      return
    }
    const dict = {}
    message.answer.forEach(answer => {
      const { ip, cname, qtype } = answer
      const name = answer.name.join('.')
      if (qtype === 5) {
        dict[name] = { cname: cname.join('.') }
      } else if (qtype === 1) {
        dict[name] = { ip: `${ip[0]}.${ip[1]}.${ip[2]}.${ip[3]}` }
      }
    })
    let ip
    let q = query
    while (!ip) {
      const res = dict[q]
      if (res.ip) {
        ip = res.ip
        break
      }
      q = res.cname
    }
    loop.remove(fd)
    net.close(fd)
    onRecord(null, ip)
  })
  const len = create(query, buf, 1)
  const rc = udp.sendmsg(fd, buf, address, port, len)
  if (rc === -1) {
    const errno = just.sys.errno()
    onRecord(new Error(`Error sending ${query} to ${address}: ${just.sys.strerror(errno)} (${errno})`))
    loop.remove(fd)
    net.close(fd)
    return
  }
  const timer = just.setTimeout(() => {
    onRecord(new Error(`Request timed out for ${query} at ${address}`))
    loop.remove(fd)
    net.close(fd)
  }, 1000)
}

const dnsMap = new Map()

function getIPAddress (hostname, map = dnsMap) {
  return new Promise((resolve, reject) => {
    if (map.has(hostname)) {
      resolve(map.get(hostname))
      return
    }
    lookup(hostname, (err, ip) => {
      if (err) {
        reject(err)
        return
      }
      map.set(hostname, ip)
      resolve(ip)
    })
  })
}

module.exports = { lookup, getIPAddress }
