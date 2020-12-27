const fs = require('fs').promises
const path = require('path')

async function main () {
    try {
        await fs.open(path.join(__filename, 'hello'))
    } catch (error) {
        console.log(error.code)
    }
    try {
        await fs.mkdir(__filename)
    } catch (error) {
        console.log(error.code)
    }
}

main()
