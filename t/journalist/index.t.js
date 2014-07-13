require('proof')(1, function (step, assert) {
    var Journalist = require('../..')
    var journalist = new Journalist
    assert(journalist, 'created')
})
