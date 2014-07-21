var fs = require('fs'),
    path = require('path'),
    rimraf = require('rimraf')

var tmp = path.resolve(__dirname, 'tmp')

module.exports = require('proof')(function (step) {
    rimraf(tmp, step())
}, function (step) {
    step(function () {
        fs.mkdir(tmp, 0755, step())
    }, function () {
        return { tmp: tmp }
    })
})
