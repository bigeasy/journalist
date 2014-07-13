var Cache = require('magazine')
function Journalist () {
    this._magazine = (new Cache).createMagazine()
}

module.exports = Journalist
