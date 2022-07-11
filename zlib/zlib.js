const { zlib } = just.library('zlib')

const { Z_DEFAULT_COMPRESSION, Z_FINISH } = zlib

function gunzip (buf, targetSize = 8 * 1024 * 1024) {
  const inflate = zlib.createInflate(buf, targetSize, 31)
  const status = [0, 0]
  const len = buf.byteLength
  const rc = zlib.writeInflate(inflate, buf, 0, len, status, Z_FINISH)
  zlib.endInflate(inflate)
  const [read, written] = status
  if (rc === 1) return inflate.slice(0, written)
  return new ArrayBuffer(0)
}

function gzip (buf, compression = Z_DEFAULT_COMPRESSION) {
  const deflate = zlib.createDeflate(buf, buf.byteLength, compression, 31)
  const bytes = zlib.writeDeflate(deflate, buf.byteLength, Z_FINISH)
  zlib.endDeflate(deflate)
  if (bytes < 0) return new ArrayBuffer(0)
  return deflate.slice(0, bytes)
}

zlib.gunzip = gunzip
zlib.gzip = gzip

module.exports = zlib
