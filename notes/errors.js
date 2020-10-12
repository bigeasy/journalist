const fs = require('fs').promises

async function main () {
    await fs.mkdir('foo')
}

main()
