// Foreground Colors
const AD = '\u001b[0m' // ANSI Default
const A0 = '\u001b[30m' // ANSI Black
const AR = '\u001b[31m' // ANSI Red
const AG = '\u001b[32m' // ANSI Green
const AY = '\u001b[33m' // ANSI Yellow
const AB = '\u001b[34m' // ANSI Blue
const AM = '\u001b[35m' // ANSI Magenta
const AC = '\u001b[36m' // ANSI Cyan
const AW = '\u001b[37m' // ANSI White

// Background Colors
const BD = '\u001b[0m' // ANSI Default
const B0 = '\u001b[40m' // ANSI Black
const BR = '\u001b[41m' // ANSI Red
const BG = '\u001b[42m' // ANSI Green
const BY = '\u001b[43m' // ANSI Yellow
const BB = '\u001b[44m' // ANSI Blue
const BM = '\u001b[45m' // ANSI Magenta
const BC = '\u001b[46m' // ANSI Cyan
const BW = '\u001b[47m' // ANSI White

function dump (bytes, len = bytes.length, off = 0, width = 16, pos = 0, decimal = false) {
  const result = []
  const chars = []
  const base = decimal ? 10 : 16
  for (let i = 0; i < len; i++) {
    if (i % width === 0) {
      if (i === 0) {
        result.push('')
      } else {
        result.push(` ${chars.join('')}\n`)
        chars.length = 0
      }
    }
    const boff = i + off
    if (i % 8 === 0) {
      result.push(`${AG}${(boff).toString(base).padStart(5, ' ')}${AD}`)
    }
    result.push(` ${bytes[boff].toString(16).padStart(2, '0')}`)
    if (bytes[boff] >= 32 && bytes[boff] <= 126) {
      chars.push(`${AC}${String.fromCharCode(bytes[boff])}${AD}`)
    } else {
      chars.push('.')
    }
  }
  const remaining = width - (len % width)
  if (remaining === width) {
    result.push(` ${chars.join('')}\n`)
  } else if (remaining < 8) {
    result.push(`${'   '.repeat(remaining)} ${chars.join('')}\n`)
  } else {
    result.push(`${'   '.repeat(remaining)}      ${chars.join('')}\n`)
  }
  return result.join('')
}

function b2ipv4 (v) {
  return `${v >> 24 & 0xff}.${v >> 16 & 0xff}.${v >> 8 & 0xff}.${v & 0xff}`
}

function ipv42b (v) {
  const [b0, b1, b2, b3] = v.split('.').map(o => parseInt(o, 10))
  return (b0 << 24) + (b1 << 16) + (b2 << 8) + b3
}

function toMAC (u8) {
  return Array.prototype.map.call(u8, v => v.toString(16).padStart(2, '0')).join(':')
}

function htons16 (n) {
  const u16 = n & 0xffff
  return (u16 & 0xff) << 8 + (u16 >> 8)
}

function getFlags (flags) {
  return Object.keys(flags).filter(v => flags[v])
}

function pad (n, p = 10) {
  return n.toString().padStart(p, ' ')
}

function tcpDump (packet) {
  const { frame, header, message, bytes, offset } = packet // eth frame, ip header, tcp message
  const size = bytes - offset
  const { seq, ack, flags } = message // get tcp fields
  const [source, dest] = [b2ipv4(header.source), b2ipv4(header.dest)] // convert source and dest ip to human-readable
  return `
${AM}Eth  ${AD}: ${AM}${toMAC(frame.source)}${AD} -> ${AM}${toMAC(frame.dest)}${AD}
${AG}${frame.protocol.padEnd(4, ' ')} ${AD}: ${AG}${source}${AD} -> ${AG}${dest}${AD}
${AY}TCP  ${AD}: ${AY}${message.source}${AD} -> ${AY}${message.dest}${AD} seq ${AY}${pad(seq)}${AD} ack ${AY}${pad(ack)}${AD} (${AC}${getFlags(flags).join(' ')}${AD}) ${size}
`.trim()
}

function udpDump (packet) {
  const { frame, header, message, bytes, offset } = packet // eth frame, ip header, udp message
  const size = bytes - offset
  const [source, dest] = [b2ipv4(header.source), b2ipv4(header.dest)] // convert source and dest ip to human-readable
  return `
${AM}Eth  ${AD}: ${AM}${toMAC(frame.source)}${AD} -> ${AM}${toMAC(frame.dest)}${AD}
${AG}${frame.protocol.padEnd(4, ' ')} ${AD}: ${AG}${source}${AD} -> ${AG}${dest}${AD}
${AY}UDP  ${AD}: ${AY}${message.source}${AD} -> ${AY}${message.dest}${AD} ${size}
`.trim()
}

const ANSI = { AD, AY, AM, AC, AG, AR, AB }
ANSI.colors = { fore: { AD, A0, AR, AG, AY, AB, AM, AC, AW }, back: { BD, B0, BR, BG, BY, BB, BM, BC, BW } }
const HOME = '\u001b[0;0H' // set cursor to 0,0
const CLS = '\u001b[2J' // clear screen
const EL = '\u001b[K' // erase line
const SAVECUR = '\u001b[s' // save cursor
const RESTCUR = '\u001b[u' // restore cursor
const HIDE = '\u001b[?25l' // hide cursor
const SHOW = '\u001b[?25h' // show cursor
ANSI.control = {
  home: () => HOME,
  move: (x = 0, y = 0) => `\u001b[${x};${y}H`,
  column: x => `\u001b[${x}G`,
  cls: () => CLS,
  eraseLine: () => EL,
  cursor: {
    hide: () => HIDE,
    show: () => SHOW,
    save: () => SAVECUR,
    restore: () => RESTCUR
  }
}

module.exports = { dump, ANSI, getFlags, htons16, toMAC, ipv42b, b2ipv4, tcpDump, udpDump }
