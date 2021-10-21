const { fs } = just.library('fs')
const { sys } = just.library('sys')
const { net } = just.library('net')
const { mkdir, open, O_TRUNC, O_CREAT, O_WRONLY, EEXIST } = fs
const { errno, strerror } = sys
const { write, close } = net

function isLastBlock (dv, off) {
  for (let n = off + 511; n >= off; --n) {
    if (dv.getUint8(n) !== 0) {
      return false
    }
  }
  return true
}

function verifyChecksum (dv, off) {
  return true
}

function getOctal (buf, off, len) {
  const { u8 } = buf
  let i = 0
  while ((u8[off] < ZERO || u8[off] > SEVEN) && len > 0) {
    len--
    off++
  }
  while (u8[off] >= ZERO && u8[off] <= SEVEN && len > 0) {
    i *= 8
    i += (u8[off] - ZERO)
    len--
    off++
  }
  return i
}

function createFile (src, off, mode) {
  const { u8 } = src
  let len = 0
  let i = off
  while (u8[i++] !== 0) len++
  if (u8[off + len - 1] === SLASH) len--
  const fileName = src.readString(len, off)
  let fd = open(`${justDir}/${fileName}`, O_TRUNC | O_CREAT | O_WRONLY, mode)
  if (fd < 0) {
    const lastSlash = fileName.lastIndexOf('/')
    if (lastSlash > -1) {
      //createDirectory(fileName.slice(0, lastSlash))
      fd = open(`${justDir}/${fileName}`, O_TRUNC | O_CREAT | O_WRONLY, mode)
    }
  }
  return fd
}

function createDirectory (src, off, mode) {
  const { u8 } = src
  let len = 0
  let i = off
  while (u8[i++] !== 0) len++
  if (u8[off + len - 1] === SLASH) len--
  const dirName = src.readString(len, off)
  let r = mkdir(`${justDir}/${dirName}`, mode)
  if (r !== 0) {
    if (errno() !== EEXIST) {
      const lastSlash = dirName.lastIndexOf('/')
      if (lastSlash > -1) {
        r = mkdir(`${justDir}/${dirName.slice(0, lastSlash)}`, mode)
      }
    }
  }
  return r
}

function writeBytes (fd, src, off, len) {
  const chunks = Math.ceil(len / 4096)
  const end = off + len
  let bytes = 0
  for (let i = 0; i < chunks; ++i, off += 4096) {
    const towrite = Math.min(end - off, 4096)
    bytes = write(fd, src, towrite, off)
    if (bytes <= 0) break
  }
  if (bytes < 0) {
    just.error(`Error Writing to File: ${errno()} (${strerror(errno())})`)
  }
  if (bytes === 0) {
    just.error(`Zero Bytes Written: ${errno()}`)
  }
  const r = close(fd)
  if (r < 0) {
    just.error(`Error Closing File: ${errno()}`)
    return r
  }
  return bytes
}

function untar2 (src, size, stat) {
  const dv = new DataView(src)
  const u8 = new Uint8Array(src)
  src.view = dv
  src.u8 = u8
  for (let off = 0; off < size; off += 512) {
    const end = off + 512
    if (end > size) {
      just.error('Short read')
      return -1
    }
    if (isLastBlock(dv, off)) {
      return 0
    }
    if (!verifyChecksum(dv, off)) {
      just.error('Checksum failed')
      return -1
    }
    let fileSize = getOctal(src, off + 124, 12)
    let fd = 0
    const fileType = dv.getUint8(off + 156)
    if (fileType === 53) {
      const mode = getOctal(src, off + 100, 8)
      createDirectory(src, off, mode)
      fileSize = 0
    } else if (fileType === 48) {
      const mode = getOctal(src, off + 100, 8)
      fd = createFile(src, off, mode)
    } else {
      //just.error(`unknown file type ${fileType}`)
    }
    if (fd < 0) {
      if (errno() !== EEXIST) {
        just.error(`bad fd: ${fd}`)
        just.error(strerror(errno()))
        return -1
      }
    }
    if (fileSize > 0) {
      writeBytes(fd, src, off + 512, fileSize)
      off += (fileSize + (512 - (fileSize % 512)))
    }
  }
  return -1
}

function untar (src, size = src.byteLength) {
  const stat = {}
  const code = untar2(src, size, stat)
  const { pathName } = stat
  return { code, pathName }
}

const ASCII0 = 48
const SLASH = '/'.charCodeAt(0)
const ZERO = '0'.charCodeAt(0)
const SEVEN = '7'.charCodeAt(0)
const justDir = `${just.env().JUST_HOME || just.sys.cwd()}`

module.exports = { untar }
