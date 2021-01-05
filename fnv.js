const fnv = require('hash.fnv')
const crypto = require('crypto')

module.exports = function (buffer, offset, length) {
    return fnv(0, buffer, offset, length)
}
