class SimpleCache {
  constructor (refresh = () => {}, expiration = 60000) {
    this.maxItemSize = 65536
    this.maxItems = 1 * 1024 * 1024
    this.now = Date.now()
    this.clock = 0
    this.resolution = 1000 // milliseconds
    this.map = new Map()
    this.defaultExpiration = expiration
    this.refresh = refresh
    this.hit = 0
    this.miss = 0
  }

  get (key, expires = this.defaultExpiration) {
    const entry = this.map.get(key)
    if (!entry) {
      // todo: stop thundering herd
      const value = this.refresh(key)
      if (!value) return null
      this.map.set(key, { value, ts: this.now })
      this.miss++
      return value
    }
    if (this.now - entry.ts < expires) {
      this.hit++
      return entry.value
    }
    // we don't need this for sync
    // stop anyone else re-fetching
    entry.ts = this.now + 5000
    // todo: we need to stop parallel requests
    const value = this.refresh(key)
    this.map.set(key, { value, ts: this.now })
    this.miss++
    return value
  }

  async getAsync (key, expires = this.defaultExpiration) {
    const entry = this.map.get(key)
    if (!entry) {
      // todo: stop thundering herd
      const value = await this.refresh(key)
      if (!value) return null
      this.map.set(key, { value, ts: this.now })
      this.miss++
      return value
    }
    if (this.now - entry.ts < expires) {
      this.hit++
      return entry.value
    }
    // stop anyone else re-fetching
    entry.ts = this.now + 5000
    // todo: we need to stop parallel requests
    const value = await this.refresh(key)
    this.map.set(key, { value, ts: this.now })
    this.miss++
    return value
  }

  delete (key) {
    this.map.delete(key)
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
