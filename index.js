var fs = require('fs'),
    path = require('path'),
    assert = require('assert'),
    cadence = require('cadence'),
    Staccato = require('staccato'),
    Cache = require('magazine')

function Entry (journal, filename, position) {
    this._journal = journal
    this._filename = filename
    this._position = position
    this._extant = false
    this._opened = true
}

Entry.prototype.write = cadence(function (step, buffer) {
    step(function () {
        this._staccato.write(buffer, step())
    }, function () {
        return this._position += buffer.length
    })
})

Entry.prototype.close = cadence(function (step, stage) {
    assert.ok(!this._closing, 'already closing')
    this._open = false
    this._closing = []
    if (this._journal._journalist._stage == stage) {
        step(function () {
            this._journal._journalist._closer(this, this._position, step())
        }, function () {
            this._staccato.close(step())
        }, function () {
            this._cartridge.remove()
            if (this._closing.length) {
                this._closing.pop()()
            }
        })
    } else {
        this._cartridge.release()
    }
})

function Journal (journalist, magazine) {
    this._journalist = journalist
    this._magazine = magazine
}

Journal.prototype.open = cadence(function (step, filename, position) {
    step(function () {
        fs.realpath(path.dirname(filename), step())
    }, function (dir) {
        var resolved = path.join(dir, path.basename(filename))
        var cartridge = this._magazine.hold(resolved, new Entry(this, resolved, position)),
            entry = cartridge.value
        entry._cartridge = cartridge
        if (!entry.extant) {
            this._open(entry, step())
        } else if (object._closing) {
            step(function () {
                object._closing.push(step())
            }, function () {
                this.open(filename, position, step())
            })
        } else {
            object._opened = true
            return entry
        }
    })
})

Journal.prototype._open = cadence(function (step, entry) {
    step(function () {
        entry._staccato = new Staccato(fs.createWriteStream(entry._filename, {
            flags: 'w+',
            mode: 0644,
            start: entry._position
        }), true)
        entry._staccato.ready(step())
    }, function () {
        return entry
    })
})

function Journalist (stage, closer, cache) {
    this._cache = cache ||(new Cache)
    this._stage = stage
    this._closer = closer
    this._count = 0
}

Journalist.prototype.createJournal = function () {
    return new Journal(this, this._cache.createMagazine())
}

Journalist.prototype.purge = cadence(function (step) {
    var gather = [], count = this._cache.count
    this._cache.purge(function (entry) {
        gather.push({
            magazine: entry._magazine,
            filename: entry.value._filename
        })
        return --count  < this._count
    }.bind(this))
    gather.forEach(step([], function (record) {
        var cartridge = record._magazine.hold(record.filename, null),
            entry = cartridge.value
        if (entry && !entry._opened) {
            entry._closing = []
        }
        console.log(gather)
    }))
})

module.exports = Journalist
