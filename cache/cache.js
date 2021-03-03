class SimpleCache {
  constructor (refresh = () => {}, expiration = 60000) {
    this.maxItemSize = 65536
    this.maxItems = 1 * 1024 * 1024
    this.now = Date.now()
    this.clock = 0
    this.resolution = 10 // milliseconds
    this.map = new Map()
    this.defaultExpiration = expiration
    this.refresh = refresh
    this.hit = 0
    this.miss = 0
  }

  async get (key, expires = 60000) {
    const entry = this.map.get(key)
    if (!entry) {
      const value = await this.refresh(key)
      this.map.set(key, { value, ts: this.now })
      this.miss++
      return value
    }
    if (this.now - entry.ts < expires) {
      this.hit++
      return entry.value
    }
    const value = await this.refresh(key)
    this.map.set(key, { value, ts: this.now })
    this.miss++
    return value
  }

  tick () {
    this.now = Date.now()
  }

  start () {
    if (this.clock) return
    const cache = this
    this.clock = just.setInterval(() => cache.tick(), this.resolution)
    return this
  }

  stop () {
    if (this.clock) just.clearInterval(this.clock)
    return this
  }
}

module.exports = { SimpleCache }
