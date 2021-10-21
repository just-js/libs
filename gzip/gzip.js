const { zlib } = just.library('zlib')
const { createInflate, writeInflate, endInflate, Z_NO_FLUSH, Z_STREAM_END } = zlib

function gunzip (src) {
  const state = [0, 0]
  const dest = createInflate(src, maxArchiveSize, 31)
  const r = writeInflate(dest, src, 0, src.byteLength, state, Z_NO_FLUSH)
  const [read, write] = state
  if (read === src.byteLength && r === Z_STREAM_END) {
    const tar = new ArrayBuffer(write)
    tar.copyFrom(dest, 0, write, 0)
    endInflate(dest, true)
    return tar
  }
}

const maxArchiveSize = just.env().MAX_ARCHIVE || (64 * 1024 * 1024)

module.exports = { gunzip }
