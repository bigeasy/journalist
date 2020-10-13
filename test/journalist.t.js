require('proof')(25, async okay => {
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

    // Recover a commit directory with no commit.
    {
        const commit = await createCommit()
        okay(await Journalist.prepare(commit), 0, 'no prepare')
        okay(await Journalist.commit(commit), 0, 'no commit')
        await commit.dispose()
    }


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
        okay(await commit.relative('hello/world.txt'), 'tmp/staging/hello/world.txt', 'aliased')
        okay(await commit.relative('missing.txt'), null, 'missing aliased')
        // await commit.rename('hello/world.txt', 'hello/world.pdf')
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), { hello: { 'world.txt': 'hello, world' } }, 'write file ')
    }

    // Create a file with hash in name.
    {
        const commit = await createCommit()
        const entry = await commit.writeFile(hash => `one.${hash}.txt`, Buffer.from('hello, world'))
        okay(entry, {
            method: 'emplace',
            filename: 'one.4d0ea41d.txt',
            options: { flag: 'wx', mode: 438, encoding: 'utf8' },
            hash: '4d0ea41d'
        }, 'entry for file with hash in filename')
        // await commit.rename('hello/world.txt', 'hello/world.pdf')
        await commit.write()
        await Journalist.prepare(commit)
        for (const operation of await commit.commit()) {
            if (await operation.commit() == 'rename') {
                break
            }
            await operation.dispose()
        }
        const recovery = await Journalist.create(directory)
        okay(await Journalist.prepare(recovery), 0, 'no recovery necessary')
        for (const operation of await recovery.commit()) {
            await operation.commit()
            await operation.dispose()
        }
        await recovery.dispose()
        okay(await list(directory), {
            'one.4d0ea41d.txt': 'hello, world'
        }, 'write file with hash in filename')
    }

    // Resolve a file name that is in the primary directory.
    {
        const commit = await createCommit()
        await create(directory, { hello: 'world' })
        okay(await commit.relative('hello'), 'hello', 'relative in primary directory')
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
    }

    // Remove a file.
    {
        const commit = await createCommit()
        await create(directory, { hello: 'world' })
        await commit.unlink('hello')
        await commit.write()
        await Journalist.prepare(commit)
        for (const operation of await commit.commit()) {
            if (await operation.commit() == 'unlink') {
                break
            }
            await operation.dispose()
        }
        const recovery = await Journalist.create(directory)
        await Journalist.prepare(recovery)
        const commits = await recovery.commit()
        okay(commits.length, 2, 'recovery of unlink')
        await Journalist.commit(recovery)
        await recovery.dispose()
        okay(await list(directory), {}, 'unlink primary')
    }

    // Remove a staged file.
    {
        const commit = await createCommit()
        await create(directory, { hello: 'world' })
        await commit.writeFile('hello', Buffer.from('world'))
        await commit.unlink('hello')
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), {}, 'unlink staged')
    }

    // Partition a commit.
    {
        const commit = await createCommit()
        await commit.writeFile('one.txt', Buffer.from('one'))
        commit.partition()
        await commit.writeFile('two.txt', Buffer.from('two'))
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        const listing = await list(directory)
        okay({
            commit: { staging: listing.tmp.staging },
            'one.txt': listing['one.txt']
        }, {
            commit: { staging: { commit: {}, 'two.txt': 'two' } },
            'one.txt': 'one'
        }, 'partial commit')
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), {
            'one.txt': 'one',
            'two.txt': 'two'
        }, 'partitioned commit')
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
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), { 'one.txt': 'two' }, 'overwite file in staging')
    }

    // Create a directory.
    {
        const commit = await createCommit()
        const errors = []
        const entry = await commit.mkdir('dir')
        try {
            await commit.mkdir('dir')
        } catch (error) {
            errors.push(error.code, error.errno)
        }
        try {
            await commit.writeFile('dir', Buffer.from('one'), { flag: 'w' })
        } catch (error) {
            errors.push(error.code, error.errno)
        }
        await fs.writeFile(path.join(await commit.absolute('dir'), 'one.txt'), 'one')
        okay(errors, [ 'EEXIST', -17, 'EISDIR', -21 ], 'overwrite existing')
        await commit.write()
        await Journalist.prepare(commit)
        for (const operation of await commit.commit()) {
            if (await operation.commit() == 'rename') {
                break
            }
            await operation.dispose()
        }
        const recovery = await Journalist.create(directory)
        await Journalist.prepare(recovery)
        for (const operation of await recovery.commit()) {
            await operation.commit()
            await operation.dispose()
        }
        await recovery.dispose()
        okay(await list(directory), { dir: { 'one.txt': 'one' } }, 'create directory')
    }

    // Catch failure to create a directory.
    {
        const commit = await createCommit()
        const entry = await commit.mkdir('dir')
        await commit.write()
        await Journalist.prepare(commit)
        for (const operation of await commit.commit()) {
            if (await operation.commit() == 'rename') {
                break
            }
            await operation.dispose()
        }
        await fs.rmdir(path.join(directory, 'dir'))
        const recovery = await Journalist.create(directory)
        await Journalist.prepare(recovery)
        const errors = []
        try {
            for (const operation of await recovery.commit()) {
                await operation.commit()
                await operation.dispose()
            }
        } catch (error) {
            errors.push(/^rename failed$/m.test(error.message))
        }
        okay(errors, [ true ], 'caught failure to create directory')
        await recovery.dispose()
    }

    // Rename a file.
    {
        const commit = await createCommit()
        const errors = []
        await create(directory, { one: 'one' })
        await commit.rename('one', 'two')
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), { two: 'one' }, 'rename file in primary')
    }

    // Rename a staged file.
    {
        const commit = await createCommit()
        const errors = []
        await create(directory, { one: 'one' })
        await commit.writeFile('two', Buffer.from('two'))
        await commit.rename('two', 'three')
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), { one: 'one', three: 'two' }, 'rename file in staging')
    }

    // TODO Really consider maybe not passing through if it is a staged
    // directory created by `mkdir`.

    // Remove a primary directory.
    {
        const commit = await createCommit()
        const errors = []
        await create(directory, { one: {}, two: {} })
        await commit.rmdir('two', 'three')
        await commit.write()
        await Journalist.prepare(commit)
        for (const operation of await commit.commit()) {
            if (await operation.commit() == 'rmdir') {
                break
            }
            await operation.dispose()
        }
        const recovery = await Journalist.create(directory)
        await Journalist.prepare(commit)
        const commits = await commit.commit()
        okay(commits.length, 2, 'mkdir recovery')
        for (const operation of commits) {
            await operation.commit()
            await operation.dispose()
        }
        await recovery.dispose()
        okay(await list(directory), { one: {} }, 'remove directory from primary')
    }

    // Remove a staged directory.
    {
        const commit = await createCommit()
        const errors = []
        await create(directory, { one: {}, two: {} })
        await commit.mkdir('two')
        await commit.rmdir('two')
        await commit.write()
        await Journalist.prepare(commit)
        await Journalist.commit(commit)
        await commit.dispose()
        okay(await list(directory), { one: {} }, 'remove directory from staging')
    }
})
