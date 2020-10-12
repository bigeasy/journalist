const fs = require('fs').promises

async function main () {
    try {
        await fs.writeFile('../journalist/foo', 'x', { flag: 'wx' })
    } catch (error) {
        console.log(error.message)
    }
}

main()
