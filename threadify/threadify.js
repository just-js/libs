const threading = just.library('thread')
const { readFile } = require('fs')
const { fileName } = require('path')

function spawn (main) {
  if (just.sys.tid() !== just.sys.pid()) {
    main().catch(err => just.error(err.stack))
    return
  }
  const cpus = parseInt(just.env().CPUS || just.sys.cpus, 10)
  if (cpus === 1) {
    // if only one thread specified, then just run main in the current thread
    main().catch(err => just.error(err.stack))
    return [just.sys.pid()]
  }
  let source = just.builtin(`${fileName(just.args[0])}.js`)
  if (!source) {
    source = readFile(just.args[1])
  }
  const threads = []
  for (let i = 0; i < cpus; i++) {
    threads.push(threading.thread.spawn(source, just.builtin('just.js'), just.args))
  }
  return threads
}

module.exports = { spawn }
