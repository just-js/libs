const threading = just.library('thread')
const { readFile } = require('fs')

const { spawn } = threading.thread

function threadify (main) {
  if (just.sys.tid() !== just.sys.pid()) {
    main().catch(err => just.error(err.stack))
    return
  }
  let source = just.builtin(`${just.args[0]}.js`)
  if (!source) {
    source = readFile(just.args[1])
  }
  const threads = []
  const cpus = parseInt(just.env().CPUS || just.sys.cpus, 10)
  for (let i = 0; i < cpus; i++) {
    threads.push(spawn(source, just.builtin('just.js'), just.args))
  }
  just.setInterval(() => {
    const { user, system } = just.cpuUsage()
    const { rss } = just.memoryUsage()
    const totalMem = Math.floor(Number(rss) / (1024 * 1024))
    const memPerThread = Math.floor((totalMem / cpus) * 100) / 100
    just.print(`threads ${threads.length} mem ${totalMem} MB / ${memPerThread} MB cpu (${user.toFixed(2)}/${system.toFixed(2)}) ${(user + system).toFixed(2)}`)
  }, 1000)
}

module.exports = { threadify }
