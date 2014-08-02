require('./proof')(6, function (step, tmp, assert) {
    var fs = require('fs'),
        path = require('path'),
        cadence = require('cadence'),
        Journalist = require('../..')
    var footerCount = 0, footer = cadence(function (step, entry, position, extra) {
        assert(extra, 1, 'footer ' + (++footerCount))
        var buffer = new Buffer(4)
        buffer.writeUInt32BE(4, 0)
        entry.write(buffer, step())
    })
    var journalist = new Journalist({ stage: 'entry', count: 1 })
    assert(journalist, 'journalist created')
    var journal = journalist.createJournal()
    assert(journal, 'journal created')
    var buffer = new Buffer(4)
    buffer.writeUInt32BE(0xaaaaaaaa, 0)
    step(function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(step())
    }, function (entry) {
        step(function () {
            entry.write(buffer, step())
        }, function (position) {
            assert(position, 4, 'position')
            entry.close('entry', step())
        })
    }, function () {
        journal.open(path.join(tmp, 'data'), 4, 1).ready(step())
    }, function (entry) {
        entry.close('entry', step())
    }, function () {
        journalist = new Journalist({ stage: 'journal', closer: footer })
        journal = journalist.createJournal()
        journal.open(path.join(tmp, 'data'), 0, 1).ready(step())
    }, function (entry) {
        var order = []
        step(function () {
            entry.write(buffer, step())
        }, function (position) {
            entry.close('entry', step())
            step(function () {
                journalist.purge(step())
            }, function () {
                order.push('purge')
            })
            step(function (filename) {
                journal.open(path.join(tmp, 'data'), 4, 1).ready(step())
            }, function (entry) {
                order.push('open')
                entry.close('entry', step())
            })
        }, function () {
            assert(order, [ 'purge', 'open' ], 'purge then open')
        })
    }, function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(step())
    }, function (entry) {
        entry.close('entry', step())
    }, function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(step())
    }, function (entry) {
        entry.close('entry', step())
    }, function () {
        journal.close('other', step())
    }, function () {
        journal.close('journal', step())
    })
})
