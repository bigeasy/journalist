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

Error.stackTraceLimit = 999

Entry.prototype._close = cadence(function (step) {
    step(function () {
        this._closing = []
    //    this._journal._journalist._closer(this, this._position, step())
    }, function () {
        this._staccato.close(step())
    }, function () {
        if (this._closing.length) {
            this._closing.pop()()
        }
        delete this._closing
    })
})

Entry.prototype.close = cadence(function (step, stage) {
    assert.ok(!this._closing, 'already closing')
    this._open = false
    if (this._journal._journalist._stage == stage) {
        step(function () {
            this._close(step())
        }, function () {
            this._cartridge.remove()
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
        if (entry._closing) {
            step(function () {
                object._closing.push(step())
            }, function () {
                this.open(filename, position, step())
            })
        } else {
            entry._opened = true
            if (!entry._extant) {
                entry._extant = true
                this._open(entry, step())
            } else {
                return entry
            }
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
    var count = this._count, cache = this._cache, purge = cache.purge()
    step([function () {
        purge.release()
    }], function () {
        var loop = step(function () {
            if (!purge.cartridge || cache.count <= count) return [ loop ]
            purge.cartridge.value._close(step())
        }, function () {
            purge.cartridge.remove()
            purge.next()
        })()
    })
})

module.exports = Journalist
