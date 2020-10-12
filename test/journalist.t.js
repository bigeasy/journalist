require('proof')(8, async okay => {
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
            options: { flag: 'wx', mode: 438, encoding: 'utf8' },
            hash: '4d0ea41d'
        }, 'write file')
        okay(await commit.filename('hello/world.txt'), 'commit/staging/hello/world.txt', 'aliased')
        // await commit.rename('hello/world.txt', 'hello/world.pdf')
        await commit.write()
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
        await commit.write()
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        okay(await list(directory), {}, 'unlink')
    }

    // Partition a commit.
    {
        const commit = await createCommit()
        await commit.writeFile('one.txt', Buffer.from('one'))
        commit.partition()
        await commit.writeFile('two.txt', Buffer.from('two'))
        await commit.write()
        await commit.prepare()
        await commit.commit()
        const listing = await list(directory)
        okay({
            commit: { staging: listing.commit.staging },
            'one.txt': listing['one.txt']
        }, {
            commit: { staging: { commit: {}, 'two.txt': 'two' } },
            'one.txt': 'one'
        }, 'partial commit')
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        okay(await list(directory), {
            'one.txt': 'one',
            'two.txt': 'two'
        }, 'unlink')
    }

    // Overwrite a staged file.
    {
        const commit = await createCommit()
        const errors = []
        await commit.writeFile('one.txt', Buffer.from('one'))
        try {
            await commit.writeFile('one.txt', Buffer.from('two'), { flag: 'wx' })
        } catch (error) {
            errors.push(error.code)
        }
        await commit.writeFile('one.txt', Buffer.from('two'), { flag: 'w' })
        okay(errors, [ 'EEXIST' ], 'overwrite existing')
        // await commit.rename('hello/world.txt', 'hello/world.pdf')
        await commit.write()
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        okay(await list(directory), { 'one.txt': 'two' }, 'overwite file in staging')
    }
})
