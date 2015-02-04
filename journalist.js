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

Entry.prototype.ready = cadence(function (async) {
    if (this._closing) {
        async(function () {
            this._closing.push(async())
            this._cartridge.release()
        }, function () {
            var vars = [ this._filename, this._position ].concat(this._vargs)
            this._journal._open(this).ready(async())
        })
    } else {
        async(function () {
            if (!this._extant) {
                this._extant = true
                this._staccato = new Staccato(fs.createWriteStream(this._filename, {
                    flags: this._position == 0 ? 'w+' : 'r+',
                    mode: 0644,
                    start: this._position
                }), true)
                this._staccato.ready(async())
            }
        }, function () {
            return this
        })
    }
})

Entry.prototype.write = cadence(function (async, buffer) {
    async(function () {
        this._staccato.write(buffer, async())
    }, function () {
        return this._position += buffer.length
    })
})

Entry.prototype._close = cadence(function (async, footer) {
    async(function () {
        this._closed = true
        this._closing = []
        if (footer) {
            var vargs = [ this, this._position ].concat(this._vargs)
            this._journal._journalist._closer.apply(null, vargs.concat(async()))
        }
    }, function () {
        this._staccato.close(async())
    }, function () {
        var closing = this._closing
        delete this._extant
        delete this._closing
        return [ closing ]
    })
})

Entry.prototype.scram = cadence(function (async) {
    if (!this._closed) {
        async(function () {
            this._close(false, async())
        }, function () {
            this._cartridge.remove()
        })
    }
})

Entry.prototype.close = cadence(function (async, stage) {
    assert.ok(!this._closing, 'already closing')
    if (this._journal._journalist._stage == stage) {
        async(function () {
            this._close(true, async())
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

var purge = cadence(function (async, container, count) {
    var purge = container.purge()
    async([function () {
        purge.release()
    }], function () {
        var loop = async(function () {
            if (!purge.cartridge || container.count <= count) return [ loop ]
            purge.cartridge.value._close(true, async())
        }, function (closing) {
            purge.cartridge.remove()
            purge.next()
            closing.length && closing.pop()()
        })()
    })
})

Journal.prototype.close = cadence(function (async, stage) {
    if (stage == this._journalist._stage) {
        async(function () {
            purge(this._magazine, 0, async())
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

Journalist.prototype.purge = cadence(function (async) {
    purge(this._cache, this._count, async())
})

module.exports = Journalist
