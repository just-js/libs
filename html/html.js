const fs = require('fs')
const { readFile, readFileBytes } = fs
const path = require('path')
const { baseName, fileName } = path

const rx = [
  [/`/g, '&#96;'], // replace backticks
  [/\$\{([^}]+)\}/g, '&dollar;{$1}'] // replace literal variables - ${x}
]

function sanitize (str) {
  return str
    .replace(rx[0][0], rx[0][1])
    .replace(rx[1][0], rx[1][1])
}

class Tokenizer {
  constructor () {
    this.tokens = []
  }

  tokenize (buf) {
    let inDirective = false
    let inName = false
    let name = []
    let last = ''
    let directive
    let start = 0
    let end = 0
    const u8 = new Uint8Array(buf)
    for (const b of u8) {
      const c = String.fromCharCode(b)
      if (inDirective) {
        if (c === '}' && last === '}') {
          if (name.length) {
            directive[directive.name ? 'value' : 'name'] = name.join('')
            name = []
          }
          this.tokens.push({ type: 'directive', value: directive })
          inDirective = false
          start = end + 1
        } else if (c !== '}') {
          if (inName) {
            if (c === ' ') {
              directive.name = name.join('')
              name = []
              inName = false
            } else {
              name.push(c)
            }
          } else {
            name.push(c)
          }
        }
      } else {
        if (c === '{' && last === '{') {
          if (end - start > 2) {
            this.tokens.push({ type: 'string', value: buf.readString(end - start - 1, start) })
          }
          inDirective = true
          directive = {}
          inName = true
        }
      }
      last = c
      end++
    }
    if (end - start > 0) {
      this.tokens.push({ type: 'string', value: buf.readString(end - start, start) })
    }
  }
}

class Parser {
  constructor (root = '') {
    this.source = []
    this.args = []
    this.command = ''
    this.depth = 0
    this.this = 'this'
    this.root = root
    this.plugins = {}
  }

  start () {
    this.source = []
    this.args = []
    this.command = ''
    this.depth = 0
    this.this = 'this'
    this.source.push("let html = ''")
  }

  finish () {
    this.source.push('return html')
  }

  parse (token) {
    const { source } = this
    const { type } = token
    if (type === 'string') {
      source.push(`html += String.raw\`${sanitize(token.value)}\``)
      return
    }
    const { name, value } = token.value
    if (name[0] === '#') {
      this.command = name.slice(1)
      if (this.command === 'template') {
        const fileName = `${this.root}${value}`
        const template = readFileBytes(fileName)
        const tokenizer = new Tokenizer()
        tokenizer.tokenize(template)
        for (const token of tokenizer.tokens) {
          this.parse(token)
        }
        return
      }
      if (this.command === 'code') {
        source.push(`html += ${value}`)
        return
      }
      if (this.command === 'arg') {
        this.args.push(value)
        return
      }
      if (this.command === 'each') {
        this.depth++
        if (value === 'this') {
          source.push(`for (const v${this.depth} of ${value}) {`)
        } else {
          source.push(`for (const v${this.depth} of ${this.this}.${value}) {`)
        }
        this.this = `v${this.depth}`
        return
      }
      if (this.plugins[this.command]) {
        this.plugins[this.command].call(this, token.value)
        return
      }
      if (this.command === 'eachField') {
        this.depth++
        if (value === 'this') {
          source.push(`for (const v${this.depth} in ${value}) {`)
          source.push(`const name = v${this.depth}`)
          source.push(`const value = ${value}[v${this.depth}]`)
        } else {
          source.push(`for (const v${this.depth} in ${this.this}.${value}) {`)
          source.push(`const name = v${this.depth}`)
          source.push(`const value = ${this.this}.${value}[v${this.depth}]`)
        }
        this.this = ''
      }
      return
    }
    if (name[0] === '/') {
      const command = name.slice(1)
      if (command === 'each') {
        source.push('}')
        this.depth--
      }
      if (command === 'eachField') {
        source.push('}')
        this.depth--
      }
      this.command = ''
      this.this = 'this'
      return
    }
    if (this.this) {
      if (name === 'this') {
        source.push(`html += ${this.this}`)
      } else {
        const variable = name.split('.')[0]
        if (this.args.some(arg => arg === variable)) {
          source.push(`html += ${name}`)
        } else {
          source.push(`html += ${this.this}.${name}`)
        }
      }
    } else {
      source.push(`html += ${name}`)
    }
  }

  all (tokens) {
    this.start()
    for (const token of tokens) {
      this.parse(token)
    }
    this.finish()
  }
}

let index = 0

function compile (template, name = 'template', root = '', plugins = {}) {
  const tokenizer = new Tokenizer()
  tokenizer.tokenize(template)
  const parser = new Parser(root)
  parser.plugins = plugins
  parser.all(tokenizer.tokens)
  // Function.apply(this, [...parser.args, parser.source.join('\n')])
  const call = just.vm.compile(parser.source.join('\n'), `${name}${index++}.js`, parser.args, [])
  return { call, tokenizer, parser, template }
}

function load (fileName, opts = { compile: true, plugins: {} }) {
  const template = readFileBytes(fileName)
  if (template === -1) return
  if (opts.compile) return compile(template, fileName, baseName(fileName), opts.plugins).call
  return template
}

module.exports = { compile, load, Tokenizer, Parser, sanitize }
