require('proof')(17, async okay => {
    const fs = require('fs').promises
    const path = require('path')

    const Journalist = require('..')
    //

    // Journalist makes atomic changes to the structure and contents of a
    // directory. It uses the atomicity of `rename` to move files into place. It
    // flushes the new files to disk using `O_SYNC`. You can do this yourself in
    // your program to ensure a single file gets put into place, but Journalist
    // will ensure that multiple new files, renames, unlinks, rmdirs take place
    // all together in order or not at all.

    // Blah, blah... Don't have the mind to write, but keep thinking of
    // admonishments.

    // A common operation to ensure the safe replacement of a file is to write
    // the new contents to a ...

    // Journalist is for making a short series of changes.

    // Journalist assumes that it is the only actor making changes to the files
    // and directories you specify.


    // There should be no race conditions in your program where Journalist has
    // to rename a file before something deletes it. This should not be a hard
    // requirement to fulfill and should require locking of an sort.

    // Journal is supposed to run immediately after it is written. If there is a
    // crash the journal is to be run immediately when the program restarts.

    // If you where to tell a journalist to rename a directory, then you deleted
    // the directory after the commit is written, journalist will fail.

    // Journalist assumes that it is the only actor making changes to the
    // structure of the target directory on the file system. This is how it
    // ensures that the journaled file operations will complete. If you emplace
    // a file, it will assert that the file does not already exist in the
    // directory before recording your emplacement. If another actor writes a
    // file to that location the garauntee that the journaled operations are
    // valid is now void.

    // This is reasonable. If you are managing a database directory that the
    // user is also using as a tmp directory, problems occur eventually. You are
    // supposed to use Journalist to manage a directory that is in the complete
    // control of your application.

    // Journalist is supposed to create an atomic action on a sensitive
    // directory. It is not

    // Although `rename` is atomic, file writes are not. If we have a system
    // failure immediately after write a file then and rename we may find that
    // the file is in place, but the contents are empty.

    //
    const directory = path.join(__dirname, 'tmp')

    async function reset () {
        await fs.rmdir(directory, { recursive: true })
    }

    async function create (directory, structure) {
        await fs.mkdir(directory, { recursive: true })
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
    /*
    {
        await reset()
        const journalist = await Journalist.create(directory)
        okay(await Journalist.commit(journalist), 0, 'no commit')
        okay(await journalist.messages(), [], 'no commit message')
        await journalist.dispose()
    }
    */

    // Create a commit message.
    {
        await reset()
        {
            const journalist = await Journalist.create(directory)
            journalist.message(Buffer.from(JSON.stringify({ hello: 'world' })))
            okay(journalist.messages, [], 'messages are not available until after prepare')
            await journalist.prepare()
            okay(journalist.messages, [], 'messages recorded after prepare')
            await journalist.commit()
            okay(journalist.messages.map(buffer => JSON.parse(String(buffer))), [{ hello: 'world' }], 'messages persist after commit')
        }
        {
            const journalist = await Journalist.create(directory)
            okay(journalist.messages.map(buffer => JSON.parse(String(buffer))), [{ hello: 'world' }], 'messages are still around as long as you don\'t dispose')
            await journalist.dispose()
        }
    }

    // `unlink` a file.
    {
        await reset()
        await create(directory, { hello: 'world' })
        const journalist = await Journalist.create(directory)
        journalist.unlink('hello')
        await journalist.prepare()
        for (const operation of journalist) {
            if (await operation.operate() == 'unlink') {
                break
            }
            await operation.advance()
        }
        const recovery = await Journalist.create(directory)
        const operations = [ ...recovery ]
        okay(operations.length, 1, 'recovery of unlink')
        for (const operation of operations) {
            await operation.operate()
            await operation.advance()
        }
        await recovery.dispose()
        okay(await list(directory), {}, 'unlink file')
    }
    //

    // Create a directory with `mkdir`.
    //
    // This is not and cannot be recursive because recursive directory
    // construction is not atomic. If you want to create a directory path in an
    // atomic fashion, create the directory path in a staging directory using
    // `fs.mkdir()`. You can then `rename` the directory with Journalist to to
    // move it into place.

    //
    {
        await reset()
        await create(directory, {})
        const journalist = await Journalist.create(directory)
        journalist.mkdir('dir')
        await journalist.prepare()
        await journalist.commit()
        await journalist.dispose()
        okay(await list(directory), { dir: {} }, 'create directory')
    }
    //

    // Remove a directory with `rmdir`.
    //
    // This is not and cannot be recursive because a recursive directory removal
    // is not atomic. If you want to recursively delete a directory in an atomic
    // fashion, `rename` the with Journalist to move it to a staging area. You
    // can then use `fs.rmdir` to delete recursively.

    //
    {
        await reset()
        await create(directory, { dir: {} })
        const journalist = await Journalist.create(directory)
        journalist.rmdir('dir')
        await journalist.prepare()
        await journalist.commit()
        await journalist.dispose()
        okay(await list(directory), {}, 'remove directory')
    }
    //

    // Rename a file.

    //
    {
        await reset()
        await create(directory, { one: 'one' })
        const journalist = await Journalist.create(directory)
        const errors = []
        journalist.rename('one', 'two')
        await journalist.prepare()
        await journalist.commit()
        await journalist.dispose()
        okay(await list(directory), { two: 'one' }, 'rename file in primary')
    }
    //

    // You must have paths created.

    //
    {
        await reset()
        {
            await create(directory, {})
            const journalist = await Journalist.create(directory)
            journalist.mkdir('one/two')
            try {
                await journalist.prepare()
            } catch (error) {
                console.log(error.stack)
                okay(error.code, 'PATH_DOES_NOT_EXIST', 'cannot create a directory where a path does not exist')
            }
        }
        await create(directory, { one: {} })
        {
            const journalist = await Journalist.create(directory)
            journalist.mkdir('one/two')
            await journalist.prepare()
            await journalist.commit()
            await journalist.dispose()
            okay(await list(directory), { one: { two: {} } }, 'path does exist')
        }
    }
    //

    // You can create the directory as part of your script.

    //
    {
        await reset()
        {
            await create(directory, {})
            const journalist = await Journalist.create(directory)
            journalist.mkdir('one')
            journalist.mkdir('one/two')
            await journalist.prepare()
            await journalist.commit()
            await journalist.dispose()
            okay(await list(directory), { one: { two: {} } }, 'path does exist')
        }
    }
    //

    // You can create the directory as part of your script.

    //
    {
        await reset()
        {
            await create(directory, { log: { active: 'entry', next: '' } })
            const journalist = await Journalist.create(directory)
            journalist.rename('log/active', 'log/previous')
            journalist.rename('log/next', 'log/active')
            journalist.message(Buffer.from('rotate'))
            await journalist.prepare()
            await journalist.commit()
            okay(await list(path.join(directory, 'log')), { active: '', previous: 'entry' }, 'first step')
        }
        {
            const journalist = await Journalist.create(directory)
            okay(journalist.messages.map(buffer => String(buffer)), [ 'rotate' ], 'previous step')
            journalist.message(Buffer.from('shift'))
            journalist.unlink('log/previous')
            await journalist.prepare()
            await journalist.commit()
            okay(await list(path.join(directory, 'log')), { active: '' }, 'second step')
        }
        {
            const journalist = await Journalist.create(directory)
            okay(journalist.messages.map(buffer => String(buffer)), [ 'shift' ], 'final step')
            await journalist.dispose()
            okay(await list(path.join(directory, 'log')), { active: '' }, 'final step')
        }
    }
})
