require('proof')(4, async okay => {
    const fs = require('fs').promises
    const path = require('path')

    const Journalist = require('..')

    const directory = path.join(__dirname, 'tmp')

    async function createCommit () {
        await fs.rmdir(directory, { recursive: true })
        // TODO Make this `async` so we can create the directory.
        return await Journalist.create(directory)
    }

    async function create (directory, structure) {
        for (const name in structure) {
            const value = structure[name]
            const resolved = path.join(directory, name)
            if (typeof value == 'string')  {
                await fs.writeFile(resolved, value, 'utf8')
            } else {
                await fs.mkdir(resolved)
                await create(resolved, value)
            }
        }
    }

    async function list (directory) {
        const listing = {}
        for (const file of (await fs.readdir(directory))) {
            const resolved = path.join(directory, file)
            const stat = await fs.stat(resolved)
            if (stat.isDirectory()) {
                listing[file] = await list(resolved)
            } else {
                listing[file] = await fs.readFile(resolved, 'utf8')
            }
        }
        return listing
    }

    // TODO Add some files to dodge.
    // await fs.mkdir(path.join(directory, 'dir'))

    // Create a file.
    {
        const commit = await createCommit()
        const entry = await commit.writeFile('hello/world.txt', Buffer.from('hello, world'))
        okay(entry, {
            method: 'emplace',
            filename: 'hello/world.txt',
            overwrite: false,
            hash: '4d0ea41d'
        }, 'write file')
        okay(await commit.filename('hello/world.txt'), 'commit/staging/hello/world.txt', 'aliased')
        // await commit.rename('hello/world.txt', 'hello/world.pdf')
        console.log('writing')
        await commit.write()
        console.log('preparing')
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        okay(await list(directory), { hello: { 'world.txt': 'hello, world' } }, 'write file ')
    }

    // Remove a file.
    {
        const commit = await createCommit()
        await create(directory, { hello: 'world' })
        await commit.unlink('hello')
        console.log(await list(directory))
        await commit.write()
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        okay(await list(directory), {}, 'unlink')
    }
})
