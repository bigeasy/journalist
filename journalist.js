// Node.js API.
const assert = require('assert')
const fs = require('fs').promises
const path = require('path')
const os = require('os')
const util = require('util')

// Mappings from `errno` to libuv error messages so we can duplicate them.
const errno = require('errno')

// Detailed exceptions with nested stack traces.
const Interrupt = require('interrupt')

// A non-cryptographic hash to assert the validity of the contents of a file.
const fnv = require('./fnv')

// Catch exceptions by type, message, property.
const rescue = require('rescue')

// Journalist is a utility for atomic file operations leveraging the atomicity
// of `unlink` and `rename` on UNIX filesystems. With it you can perform a set
// of file operations that must occur together or not at all as part of a
// transaction. You create a script of file operations and once you commit the
// script you can be certain that the operations will be performed even if your
// program crash restarts, even if the crash restart is due to a full disk.
//
// Once you successfully commit, if the commit fails due to a full disk it can
// be resumed once you've made space on the disk and restarted your program.
//
// Journalist has resonable limitations because it is primarily an atomicity
// utility and not a filesystem utility.
//
// Journalist operates on a specific directory and will not operate outside the
// directory. It will not work across UNIX filesystems, so the directory should
// not include mounted filesystems or if it does, do not use Journalist to write
// to those mounted filesystems.
//
// Journalist file path are arguments must be relative paths. Journalist does
// not perform extensive checking of those paths, it assumes that you've
// performed path sanitation and are not using path names entered from a user
// interface. A relative path that resolves to files outside of the directory is
// certain to cause problems. Also, I've only ever use old school UNIX filenames
// in the ASCII so I'm unfamiliar with the pitfalls of internationalization,
// emojiis and the like.
//
// Journalist is not for general purpose file system operations. It is for
// creating atomic transactions. Consider it a write-only interface to the file
// system to be used with a directory structure fully under your control.

//
class Journalist {
    static Error = Interrupt.create('Journalist.Error')

    constructor (directory, { tmp, prepare }) {
        assert(typeof directory == 'string')
        this._index = 0
        this.directory = directory
        this._staged = {}
        this._commit = path.join(directory, tmp)
        this._relative = {
            tmp: tmp,
            staging: path.join(tmp, 'staging'),
            commit: path.join(tmp, 'commit')
        }
        this._absolute = {
            directory,
            tmp: path.join(directory, tmp),
            prepare: path.join(directory, tmp, 'prepare'),
            staging: path.join(directory, tmp, 'staging'),
            commit: path.join(directory, tmp, 'commit')
        }
        this._operations = []
    }

    async _create () {
        await fs.mkdir(this._absolute.directory, { recursive: true })
    }

    // TODO Should be a hash of specific files to filter, not a regex.
    async write () {
        const dir = await this._readdir()
        const unemplaced = dir.filter(file => ! /\d+\.\d+-\d+\.\d+\.[0-9a-f]/)
        assert.deepStrictEqual(unemplaced, [], 'commit directory not empty')
        await this._write('commit', this._operations)
    }

    // Believe we can just write out into the commit directory, we don't need to
    // move a file into the directory. No, we do want to get a good write and
    // only rename is atomic. What if we had a bad write?

    //
    async _prepare (operation) {
        await this._write(String(this._index++), [ operation ])
    }

    // Recall that `fs.writeFile` overwrites without complaint.

    //
    async _write (file, entries) {
        const buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
        const write = path.join(this._absolute.commit, 'write')
        await fs.writeFile(write, buffer)
        await fs.rename(write, path.join(this._commit, `${file}.${fnv(buffer)}`))
    }

    async _load (file) {
        const buffer = await fs.readFile(path.join(this._commit, file))
        const hash = fnv(buffer)
        assert.equal(hash, file.split('.')[1], 'commit hash failure')
        return buffer.toString().split('\n').filter(line => line != '').map(JSON.parse)
    }

    async _readdir () {
        await fs.mkdir(this._absolute.commit, { recursive: true })
        const dir = await fs.readdir(this._commit)
        return dir.filter(file => ! /^\./.test(file))
    }

    _path (file) {
        return path.join(this._commit, file)
    }

    _unoperate (filename) {
        const staged = this._staged[filename]
        const operation = staged.operation
        this._operations.splice(this._operations.indexOf(operation), 1)
        delete this._staged[filename]
        return operation
    }

    // `unlink` will unlink a file in the staging and primary directory. Unlike
    // the Node.js `fs.unlink`, `Journalist.unlink` will not raise an exception
    // if the file does not exist.
    //
    // If the file has been created in this Journalist using `writeFile` the
    // temporary file will be unlinked from the staging directory and it will
    // not be copied into place during commit. Regardless of whether or not a
    // staging file is unlinked, an unlink will be attempted in the primary
    // directory.

    // `unlink`  will only work on files and not directories. For directories
    // use `rmdir`.

    //
    async unlink (filename) {
        const relative = path.normalize(filename)
        if (relative in this._staged) {
            await this._unlink(this._staged[relative].absolute)
            this._unoperate(relative)
        }
        const operation = { method: 'unlink', path: filename }
        this._operations.push(operation)
        this._staged[filename] = {
            staged: false,
            relative: filename,
            operation: operation
        }
    }

    // `rmdir` will remove a directory in the staging and primary directory.
    // The directory removal is always recursive destroying the directory and
    // all its contents.
    //
    // It will run in both the primary directory during commit and the staging
    // directory regardless of whether the or not the directory has been
    // explicitly created in the staging directory with `mkdir` or created as
    // part of recursive directory creation for a `mkdir`, `rename` or
    // `writeFile` that included directories in it's path.
    //
    // Running `rmdir` in primary when the intent is to delete a directory in
    // staging should not be destructive of the primary directory contents since
    // in order to create a directory in in staging that would have been moved
    // into primary there you would have had ensure that there is no file or
    // directory in primary, so you commit script should have moved anything out
    // of the way before the `rmdir` runs. If this is confusing, the
    // alternative, running `rmdir` in staging only when directory was created
    // in staging using `Journalist.mkdir` and not implicitly is just as
    // difficult to document. (**TODO** Or is it? Try writing that
    // documentation. Maybe it works and maybe it works for `unlink` as well.
    // More unit tests first.)
    //
    // Note that if you've created files in a directory created using `mkdir` or
    // otherwise created as part of recursive directory creation, their commit
    // script operations will not be removed from the commit script and an error
    // will likely occur during commit when the files are not moved from staging
    // into the primary directory. You should only use `rmdir` to remove
    // directories from the primary directory or directories created in staging
    // with `mkdir`.
    //
    // If you do use `rmdir` to prune implicitly created directories, you must
    // use `unlink` to remove any files write to or renamed into in the
    // directory or any of its subdirectories first.

    //
    async rmdir (filename) {
        const relative = path.normalize(filename)
        if (relative in this._staged) {
            this._unoperate(relative)
        }
        await fs.rmdir(path.join(this._absolute.staging, relative), { recursive: true })
        const operation = { method: 'rmdir', path: filename }
        this._operations.push(operation)
        this._staged[filename] = {
            staged: false,
            operation: operation
        }
    }

    async _unlink (file) {
        try {
            await fs.unlink(file)
        } catch (error) {
            rescue(error, [{ code: 'ENOENT' }])
        }
    }

    async _filename (filename) {
        const relative = path.normalize(filename)
        if (this._staged[relative]) {
            return this._staged[filename]
        }
        try {
            await fs.stat(path.join(this.directory, relative))
            return { relative, staged: false, operation: null }
        } catch (error) {
            rescue(error, [{ code: 'ENOENT' }])
            return { relative: null, absolute: null }
        }
    }

    async relative (filename) {
        return (await this._filename(filename)).relative
    }

    async absolute (filename) {
        return (await this._filename(filename)).absolute
    }

    // `filename` can be either a string or a function `format (hash) {}` that
    // will format a file name using the hash value of the file. The function
    // will receive the hash value as the first and only argument to the
    // function.
    //
    // `buffer` is the `Buffer` to wirte. Unlike Node.js `fs.writeFile` this
    // function does expect a `Buffer` and does not convert from `String`,
    // `TypedArray` nor `DataView`.
    //
    // Accepts an optional `encoding` with a default value of `'utf8'`.
    //
    // Accepts a `node` option which defaults to `0o666`.
    //
    // Accepts a `mode` and `flag` property, the only allowed `flag` values are
    // `'w' which will overwrite and `'wx'` which will fail if the file exists.
    // The default is `'wx'` instead of the default `'w'` of `fs.fileWrite`. If
    // writing to a staged file with `'wx'` the `EEXISTS` error is raised
    // immediately, otherwise the error is raised during commit.
    //
    // The staged file is moved into place during the commit using `rename`. The
    // `rename` operation is an atomic operation.
    //
    // Remember that this is not a file system library but an atomicity library.
    // You should only be writing files to be moved into place or if overwriting
    // files they should be less than `PIPEBUF` bytes long. (If anyone want to
    // convince me they can be longer please do.) Appends are not supported.

    //
    async writeFile (formatter, buffer, { flag = 'wx', mode = 438, encoding = 'utf8' } = {}) {
        Journalist.Error.assert(flag == 'w' || flag == 'wx', 'invalid flag')
        const options = { flag, mode, encoding }
        const hash = fnv(buffer)
        const abnormal = typeof formatter == 'function' ? formatter(hash) : formatter
        const filename = path.normalize(abnormal)
        if (filename in this._staged) {
            if (flag == 'wx') {
                throw this._error('EEXIST', 'open', filename)
            }
            if (this._staged[filename].directory) {
                throw this._error('EISDIR', 'open', filename)
            }
                this._unoperate(filename)
        }
        const temporary = path.join(this._absolute.staging, filename)
        await fs.mkdir(path.dirname(temporary), { recursive: true })
        await fs.writeFile(temporary, buffer, options)
        const operation = { method: 'emplace', filename, hash, options }
        this._staged[filename] = {
            staged: true,
            directory: false,
            relative: path.join(this._relative.staging, filename),
            absolute: path.join(this._absolute.staging, filename),
            operation: operation
        }
        this._operations.push(operation)
        return operation
    }

    // Create a directory. The directory is created in the staging area. Files
    // can then be written to the directory using the normal Node.js file system
    // module. The entire directory is moved into primary directory usign
    // `rename`. The `rename` operation is an atomic operation.
    //
    // Accepts a `mode` option which defaults to `0x777`.
    //
    // Directory construction is always recursive. The full path to the
    // directory is created if it does not already exist.

    //
    async mkdir (dirname, { mode = 0o777 } = {}) {
        const filename = path.normalize(dirname)
        if (filename in this._staged) {
            throw this._error('EEXIST', 'mkdir', filename)
        }
        const options = { mode, recursive: true }
        const temporary = path.join(this._absolute.staging, filename)
        await fs.mkdir(temporary, { recursive: true })
        const operation = { method: 'emplace', filename, hash: null, options }
        this._staged[filename] = {
            staged: true,
            directory: true,
            relative: path.join(this._relative.staging, filename),
            absolute: path.join(this._absolute.staging, filename),
            operation: operation
        }
        this._operations.push(operation)
        return operation
    }

    partition () {
        this._operations.push({ method: 'partition' })
    }

    _error (code, f, path) {
        const description = errno.code[code].description
        const error = new Error(`${code}: ${description}, ${f} ${util.inspect(path)}`)
        error.code = code
        error.errno = -os.constants.errno[code]
        error.path = path
        Error.captureStackTrace(error, Journalist.prototype._error)
        return error
    }

    // Rename a file.
    //
    // If the file is staged it will be renamed in the staging directory and
    // will then be emplaced. The emplacement will occur at this point in the
    // commit script, not at the point where the file was written using
    // `writeFile`.
    //
    // If the file is not staged the rename will be applied to the primary
    // directory during commit.
    //
    // This file operation will create any directory specified in the
    // destination path in the primary directory.
    //
    // If the destination path includes directories, any directories that do not
    // already exist in the primary directory will be created.
    //
    // Note that a staged file that has directories in its file path will not
    // remove the directories. This leaves directories in the staging directory
    // and can cause problems if you attempt to write a file to staging and one
    // of these orphaned directories is in its place.
    //
    // *TODO* Maybe `rmdir` will run recursively in the staging directory
    // regardless of whether or not there is a staging entry.

    //
    async rename (from, to) {
        const relative = { from: path.normalize(from), to: path.normalize(to) }
        if (relative.from in this._staged) {
            const absolute = {
                from: path.join(this._absolute.staging, relative.from),
                to: path.join(this._absolute.staging, relative.to)
            }
            await fs.mkdir(path.dirname(absolute.to), { recursive: true })
            await fs.rename(absolute.from, absolute.to)
            const operation = this._unoperate(relative.from)
            operation.filename = relative.to
            this._operations.push(operation)
        } else {
            const operation = {
                method: 'rename',
                from: {
                    relative: relative.from,
                    absolute: path.join(this.directory, relative.from)
                },
                to: {
                    relative: relative.to,
                    absolute: path.join(this.directory, relative.to)
                }
            }
            this._operations.push(operation)
            this._staged[relative.to] = {
                staged: false,
                operation: operation
            }
        }
    }

    // Okay. Now I see. I wanted the commit to be light and easy and minimal, so
    // that it could be written quickly and loaded quickly, but that is only
    // necessary for the leaf. We really want a `Prepare` that will write files
    // for branches instead of this thing that duplicates, but now I'm starting
    // to feel better about the duplication.
    //
    // Seems like there should be some sort of prepare builder class, especially
    // given that there is going to be emplacements followed by this prepare
    // call, but I'm content to have that still be an array.

    //
    async prepare () {
        const dir = await this._readdir()
        const commit = dir.filter(file => /^commit\.[0-9a-f]+$/.test(file)).shift()
        if (commit == null) {
            return []
        }
        const writes = []
        const operations = await this._load(commit)
        // Start by deleting the commit script, once this runs we have to move
        // forward through the entire commit.
        await writes.push([ 'begin' ])
        while (operations.length != 0) {
            const operation = operations.shift()
            assert(!Array.isArray(operation))
            switch (operation.method) {
            // This is the next commit in a series of commits, we write out the
            // remaining operations into a new commit.
            case 'partition': {
                    // TODO Come back and think about this. You want this to be
                    // garaunteed to execute, each time, so maybe you write out
                    // and `end` and `begin` pair for the parition.
                    //
                    // For now let's do this as cheaply as possible.
                    //
                    // Okay, it works. We stage a commit file and move it into
                    // place.
                    const entries = operations.splice(0)
                    const buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
                    const hash = fnv(buffer)
                    const file = `commit.${hash}`
                    const absolute = path.join(this._absolute.staging, 'commit', file)
                    await fs.mkdir(path.dirname(absolute), { recursive: true })
                    await fs.writeFile(absolute, buffer)
                    writes.push([
                        'rename',
                        path.join(this._relative.staging, 'commit', file),
                        path.join('commit', file),
                        hash
                    ])
                }
                break
            case 'emplace': {
                    const { page, hash, filename, options } = operation
                    if (options.flag == 'w') {
                        await this._prepare([ 'unlink', filename ])
                    }
                    writes.push([ 'rename', path.join(this._relative.staging, filename), filename, hash ])
                }
                break
            case 'rename': {
                    const { from, to } = operation
                    const buffer = await fs.readFile(from.absolute)
                    writes.push([ 'rename', from.relative, to.relative, fnv(buffer) ])
                }
                break
            case 'unlink': {
                    writes.push([ 'unlink', operation.path ])
                }
                break
            case 'rmdir': {
                    writes.push([ 'rmdir', operation.path ])
                }
                break
            }
        }
        await writes.push([ 'end' ])
        return writes.map(write => {
            return { prepare: () => this._prepare(write) }
        })
    }

    async __commit (step, dir) {
        const operation = (await this._load(step)).shift()
        switch (operation.shift()) {
        case 'begin':
            const commit = dir.filter(function (file) {
                return /^commit\./.test(file)
            }).shift()
            await fs.unlink(this._path(commit))
            break
        case 'rename': {
                const from = path.join(this.directory, operation.shift())
                const to = path.join(this.directory, operation.shift())
                await fs.mkdir(path.dirname(to), { recursive: true })
                // When replayed from failure we'll get `ENOENT`.
                await fs.rename(from, to)
                const hash = operation.shift()
                if (hash == null) {
                    const stat = await async function () {
                        try {
                             return await fs.stat(to)
                        } catch (error) {
                            throw new Journalist.Error('rename failed', error)
                        }
                    } ()
                    Journalist.Error.assert(stat.isDirectory(), 'rename failed')
                } else {
                    const buffer = await fs.readFile(to)
                    // TODO Is there a suitable UNIX exception?
                    Journalist.Error.assert(hash == fnv(buffer), 'rename failed')
                }
            }
            break
        case 'unlink':
            await this._unlink(path.join(this.directory, operation.shift()))
            break
        case 'rmdir':
            await fs.rmdir(path.join(this.directory, operation.shift()), { recursive: true })
            break
        case 'end':
            break
        }
    }

    // Appears that prepared files are always going to be a decimal integer
    // followed by a hexidecimal integer. Files for emplacement appear to have a
    // hyphen in them.
    //
    async commit () {
        const dir = await this._readdir()
        const steps = dir.filter(file => {
            return /^\d+\.[0-9a-f]+$/.test(file)
        }).map(file => {
            const split = file.split('.')
            return { index: +split[0], file: file, hash: split[1] }
        }).sort((left, right) => left.index - right.index)
        return steps.map(step => {
            return {
                commit: () => this.__commit(step.file, dir),
                dispose: () => fs.unlink(this._path(step.file))
            }
        })
    }

    async dispose () {
        await fs.rmdir(this._absolute.tmp, { recursive: true })
    }
}

exports.create = async function (directory, { tmp = 'commit' } = {}) {
    const journalist = new Journalist(directory, { tmp })
    await journalist._create()
    return journalist
}

exports.prepare = async function (journalist) {
    for (const operation of await journalist.prepare()) {
        await operation.prepare()
    }
}

exports.commit = async function (journalist) {
    for (const operation of await journalist.commit()) {
        await operation.commit()
        await operation.dispose()
    }
}
