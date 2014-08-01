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
}

Entry.prototype.ready = cadence(function (step) {
    if (this._closing) {
        step(function () {
            this._closing.push(step())
            this._cartridge.release()
        console.log('here', this._closing)
            console.log(this._journal._waiting)
            this._journal._waiting && this._journal._waiting()
        }, function () {
            var vars = [ this._filename, this._position ].concat(this._vargs)
            this._journal._open(this).ready(step())
        })
    } else {
        step(function () {
            if (!this._extant) {
                this._extant = true
                this._staccato = new Staccato(fs.createWriteStream(this._filename, {
                    flags: this._position == 0 ? 'w+' : 'r+',
                    mode: 0644,
                    start: this._position
                }), true)
                this._staccato.ready(step())
            }
        }, function () {
            return this
        })
    }
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
        var vargs = [ this, this._position ].concat(this._vargs)
        this._closing = []
        this._journal._journalist._closer.apply(null, vargs.concat(step()))
    }, function () {
        this._staccato.close(step())
    }, function () {
        var closing = this._closing
        delete this._extant
        delete this._closing
        return [ closing ]
    })
})

Entry.prototype.close = cadence(function (step, stage) {
    assert.ok(!this._closing, 'already closing')
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

Journal.prototype.open = function (filename, position) {
    var vargs = slice.call(arguments, 2)
    var entry = new Entry(this, filename, position, vargs)
    return this._open(entry)
}

Journal.prototype._open = function (entry) {
    var cartridge = this._magazine.hold(entry._filename, entry),
        entry = cartridge.value
    entry._cartridge = cartridge
    return entry
}

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
