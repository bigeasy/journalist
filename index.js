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

Entry.prototype._open = cadence(function (step) {
    this._staccato = new Staccato(fs.createWriteStream(this._filename, {
        flags: 'w+',
        mode: 0644,
        start: this._position
    }), true)
    this._staccato.ready(step())
})

Entry.prototype.write = cadence(function (step, buffer) {
    step(function () {
        this._staccato.write(buffer, step())
    }, function () {
        return this._position += buffer.length
    })
})

Entry.prototype._close = cadence(function (step) {
    step(function () {
        this._closing = []
        this._journal._journalist._closer(this, this._position, step())
    }, function () {
        this._staccato.close(step())
    }, function () {
        return [ this._closing ]
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
        var cartridge = this._magazine.hold(filename, new Entry(this, filename, position)),
            entry = cartridge.value
        entry._cartridge = cartridge
        if (entry._closing) {
            step(function () {
                entry._closing.push(step())
                cartridge.release()
                this._waiting && this._waiting()
            }, function () {
                this.open(filename, position, step())
            })
        } else {
            step(function () {
                entry._opened = true
                if (!entry._extant) {
                    entry._extant = true
                    entry._open(step())
                }
            }, function () {
                return entry
            })
        }
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
        }, function (closing) {
            purge.cartridge.remove()
            purge.next()
            closing.length && closing.pop()()
        })()
    })
})

module.exports = Journalist
