var fs = require('fs'),
    path = require('path'),
    assert = require('assert'),
    cadence = require('cadence'),
    Staccato = require('staccato'),
    Cache = require('magazine'),
    slice = [].slice

function Entry (journal, filename, position, vargs) {
    this._journal = journal
    this._filename = filename
    this._position = position
    this._vargs = vargs
    this._extant = false
    this._opened = true
}

Entry.prototype._open = cadence(function (step) {
    this._staccato = new Staccato(fs.createWriteStream(this._filename, {
        flags: this._position == 0 ? 'w+' : 'r+',
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
        this._journal._journalist._closer.apply(null, [ this, this._position ].concat(this._vargs, step()))
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
    var vargs = slice.call(arguments, 3)
    step(function () {
        fs.realpath(path.dirname(filename), step())
    }, function (dir) {
        var cartridge = this._magazine.hold(filename, new Entry(this, filename, position, vargs)),
            entry = cartridge.value
        entry._cartridge = cartridge
        if (entry._closing) {
            step(function () {
                entry._closing.push(step())
                cartridge.release()
                this._waiting && this._waiting()
            }, function () {
                this.open.apply(this, [ filename, position ].concat(vargs, step()))
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

var purge = cadence(function (step, container, count) {
    var purge = container.purge()
    step([function () {
        purge.release()
    }], function () {
        var loop = step(function () {
            if (!purge.cartridge || container.count <= count) return [ loop ]
            purge.cartridge.value._close(step())
        }, function (closing) {
            purge.cartridge.remove()
            purge.next()
            closing.length && closing.pop()()
        })()
    })
})

Journal.prototype.close = cadence(function (step, stage) {
    if (stage == this._journalist._stage) {
        step(function () {
            purge(this._magazine, 0, step())
        }, function () {
            assert.equal(this._magazine.count, 0, 'locks held at close')
        })
    }
})

function Journalist (options) {
    this._stage = options.stage
    this._cache = options.cache ||(new Cache)
    this._closer = options.closer || function () { slice.call(arguments).pop()() }
    this._count = options.count || 0
}

Journalist.prototype.createJournal = function () {
    return new Journal(this, this._cache.createMagazine())
}

Journalist.prototype.purge = cadence(function (step) {
    purge(this._cache, this._count, step())
})

module.exports = Journalist
