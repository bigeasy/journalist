var path = require('path'),
    rimraf = require('rimraf')

module.exports = require('proof')(function (step) {
    var tmp = path.resolve(__dirname, 'tmp')
    step(function () {
        rimraf(tmp, step())
    }, function () {
        fs.mkdir(tmp, 0755, step())
    }, function () {
        return { tmp: tmp }
    })
})
