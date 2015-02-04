require('proof')(6, require('cadence/redux')(prove))

function prove (async, assert) {
    var fs = require('fs'),
        rimraf = require('rimraf'),
        mkdirp = require('mkdirp'),
        path = require('path'),
        cadence = require('cadence'),
        Journalist = require('../..'),
        tmp = path.join(__dirname, 'tmp')
    var cleanup = cadence(function (async) {
        var rimraf = require('rimraf')
        async([function () {
            rimraf(tmp, async())
        }, function (_, error) {
            if (error.code != "ENOENT") throw error
        }])
    })
    var footerCount = 0, footer = cadence(function (async, entry, position, extra) {
        assert(extra, 1, 'footer ' + (++footerCount))
        var buffer = new Buffer(4)
        buffer.writeUInt32BE(4, 0)
        entry.write(buffer, async())
    })
    var journalist = new Journalist({ stage: 'entry', count: 1 })
    assert(journalist, 'journalist created')
    var journal = journalist.createJournal()
    assert(journal, 'journal created')
    var buffer = new Buffer(4)
    buffer.writeUInt32BE(0xaaaaaaaa, 0)
    async(function () {
        cleanup(async())
    }, function () {
        mkdirp(tmp, 0755, async())
    }, function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(async())
    }, function (entry) {
        async(function () {
            entry.write(buffer, async())
        }, function (position) {
            assert(position, 4, 'position')
            entry.close('entry', async())
        })
    }, function () {
        journal.open(path.join(tmp, 'data'), 4, 1).ready(async())
    }, function (entry) {
        entry.close('entry', async())
    }, function () {
        journalist = new Journalist({ stage: 'journal', closer: footer })
        journal = journalist.createJournal()
        journal.open(path.join(tmp, 'data'), 0, 1).ready(async())
    }, function (entry) {
        var order = []
        async(function () {
            entry.write(buffer, async())
        }, function (position) {
            entry.close('entry', async())
            async(function () {
                journalist.purge(async())
            }, function () {
                order.push('purge')
            })
            async(function (filename) {
                journal.open(path.join(tmp, 'data'), 4, 1).ready(async())
            }, function (entry) {
                order.push('open')
                entry.close('entry', async())
            })
        }, function () {
            assert(order, [ 'purge', 'open' ], 'purge then open')
        })
    }, function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(async())
    }, function (entry) {
        entry.close('entry', async())
    }, function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(async())
    }, function (entry) {
        entry.close('entry', async())
    }, function () {
        journal.close('other', async())
    }, function () {
        journal.close('journal', async())
    }, function () {
        journal.open(path.join(tmp, 'data'), 0, 1).ready(async())
    }, function (entry) {
        entry.scram(async())
        entry.scram(async())
    }, function () {
        if (!('UNTIDY' in process.env)) {
            cleanup(async())
        }
    })
}
