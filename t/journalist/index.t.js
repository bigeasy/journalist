require('./proof')(4, function (step, tmp, assert) {
    var fs = require('fs'),
        path = require('path'),
        cadence = require('cadence'),
        Journalist = require('../..')
    var pause, footer = cadence(function (step, entry, position) {
        var callback = step()
        function resume () {
            pause = false
            var buffer = new Buffer(4)
            buffer.writeUInt32BE(4, 0)
            entry.write(buffer, callback)
        }
        if (pause) pause = resume
        else resume()
    })
    var journalist = new Journalist('record', footer)
    assert(journalist, 'journalist created')
    var journal = journalist.createJournal()
    assert(journal, 'journal created')
    var buffer = new Buffer(4)
    buffer.writeUInt32BE(0xaaaaaaaa, 0)
    step(function () {
        journal.open(path.join(tmp, 'data'), 0, step())
    }, function (entry) {
        step(function () {
            entry.write(buffer, step())
        }, function (position) {
            assert(position, 4, 'position')
            entry.close('record', step())
        })
    }, function () {
        journal.open(path.join(tmp, 'data'), 4, step())
    }, function (entry) {
        entry.close('record', step())
    }, function () {
        journalist = new Journalist('database', footer)
        journal = journalist.createJournal()
        journal.open(path.join(tmp, 'data'), 0, step())
    }, function (entry) {
        var order = []
        step(function () {
            entry.write(buffer, step())
        }, function (position) {
            entry.close('record', step())
            pause = true
            step(function () {
                journalist.purge(step())
                journal._waiting = pause
            }, function () {
                order.push('purge')
            })
            step(function (filename) {
                journal.open(path.join(tmp, 'data'), 4, step())
            }, function (entry) {
                order.push('open')
                entry.close('record', step())
            })
        }, function () {
            assert(order, [ 'purge', 'open' ], 'purge then open')
        })
    })
})
