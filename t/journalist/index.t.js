require('./proof')(3, function (step, tmp, assert) {
    var path = require('path'),
        cadence = require('cadence'),
        Journalist = require('../..')
    var footer = cadence(function (step, entry, position) {
        var buffer = new Buffer(4)
        buffer.writeUInt32BE(4, 0)
        entry.write(buffer, step())
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
        step(null)
        journalist = new Journalist('database', footer)
        journal = journalist.createJournal()
        journal.open(path.join(tmp, 'data'), 0, step())
    }, function (entry) {
        step(function () {
            entry.write(buffer, step())
        }, function (position) {
            entry.close('record', step())
            journalist.purge(step())
            journal.open(path.join(tmp, 'data'), 0, step())
        })
    })
})
