const fnv = require('hash.fnv')
const crypto = require('crypto')

module.exports = function (buffer) {
    return crypto.createHash('sha1').update(buffer).digest('hex')
}
