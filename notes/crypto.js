const HashFNV = require('hash.fnv.crypto')
const fnv = require('hash.fnv')

const hash = new HashFNV(0)

hash.update('a')
hash.update('b')

console.log(hash.digest('hex'))
const buffer = Buffer.from('ab')
console.log(Number(fnv(0, buffer, 0, buffer.length)).toString(16))
