const fs = require('fs')
const { readFile } = fs

class Tokenizer {
  constructor () {
    this.tokens = []
  }

  tokenize (s) {
    let inDirective = false
    let curly = 0
    let inName = false
    let chars = []
    let name = []
    let directive
    for (const c of s) {
      if (c === '{' || c === '}') {
        curly++
        continue
      } else {
        if (curly === 1) curly = 0
      }
      if (!inDirective && curly === 2) {
        this.tokens.push({ type: 'string', value: chars.join('') })
        directive = {}
        inDirective = true
        inName = true
        curly = 0
      } else if (inDirective && curly === 2) {
        if (name.length) {
          directive[directive.name ? 'value' : 'name'] = name.join('')
          name = []
        }
        this.tokens.push({ type: 'directive', value: directive })
        inDirective = false
        chars = []
        curly = 0
      }
      if (inDirective) {
        if (inName) {
          if (c === ' ') {
            directive.name = name.join('')
            name = []
            inName = false
            continue
          }
        }
        name.push(c)
        continue
      }
      chars.push(c)
    }
    if (chars.length) {
      this.tokens.push({ type: 'string', value: chars.join('') })
    }
  }
}

class Parser {
  constructor () {
    this.source = []
    this.args = []
    this.command = ''
    this.depth = 0
    this.this = 'this'
  }

  parse (tokens) {
    const { source } = this
    this.this = 'this'
    source.push("let html = ''")
    for (const token of tokens) {
      const { type } = token
      if (type === 'string') {
        source.push(`html += \`${token.value}\``)
        continue
      }
      const { name, value } = token.value
      if (name[0] === '#') {
        this.command = name.slice(1)
        if (this.command === 'each') {
          this.depth++
          if (value === 'this') {
            source.push(`for (const v${this.depth} of ${value}) {`)
          } else {
            source.push(`for (const v${this.depth} of ${this.this}.${value}) {`)
          }
          this.this = `v${this.depth}`
          continue
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
        continue
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
        continue
      }
      if (this.this) {
        source.push(`html += ${this.this}.${name}`)
      } else {
        source.push(`html += ${name}`)
      }
    }
    source.push('return html')
  }
}

let index = 0

function compile (template, name = 'template') {
  const tokenizer = new Tokenizer()
  tokenizer.tokenize(template)
  const parser = new Parser()
  parser.parse(tokenizer.tokens)
  // Function.apply(this, [...parser.args, parser.source.join('\n')])
  const call = just.vm.compile(parser.source.join('\n'), `${name}${index++}.js`, parser.args, [])
  return { call, tokenizer, parser }
}

function load (fileName, opts = { compile: true }) {
  const template = readFile(fileName)
  if (opts.compile) return compile(template, fileName).call
  return template
}

module.exports = { compile, load, Tokenizer, Parser }
