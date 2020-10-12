// Node.js API.
const assert = require('assert')
const fs = require('fs').promises
const path = require('path')
const os = require('os')

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
        this.__prepare = prepare
    }

    async _create () {
        await fs.mkdir(this._absolute.directory, { recursive: true })
    }

    // TODO Should be a hash of specific files to filter, not a regex.
    async write () {
        const dir = await this._readdir()
        const unemplaced = dir.filter(file => ! /\d+\.\d+-\d+\.\d+\.[0-9a-f]/)
        assert.deepStrictEqual(unemplaced, [], 'commit directory not empty')
        await this._write('commit', this.__prepare)
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

    async unlink (filename) {
        const resolved = await this._filename(filename)
        if (resolved.staged) {
        } else {
            this.__prepare.push({ method: 'unlink', path: filename })
            this._staged[filename] = {
                removed: true,
                staged: false,
                relative: filename
            }
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
                const error = new Error
                error.code = 'EEXIST'
                error.errno = -os.constants.errno.EEXIST
                error.path = filename
                throw error
            }
            if (this._staged[filename].directory) {
                const error = new Error
                error.code = 'EISDIR'
                error.errno = -os.constants.errno.EISDIR
                error.path = filename
                throw error
            }
            this.__prepare.splice(this.__prepare.indexOf(this._staged[filename].operation), 1)
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
        this.__prepare.push(operation)
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
            throw this._error(new Error, 'EEXIST', filename)
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
        this.__prepare.push(operation)
        return operation
    }

    partition () {
        this.__prepare.push({ method: 'partition' })
    }

    _error (error, code, path) {
        error.code = code
        error.errno = -os.constants.errno[code]
        error.path = path
        Error.captureStackTrace(error, Journalist.prototype._error)
        return error
    }

    // This file operation will create any directory specified in the
    // destination path.

    //
    async rename (from, to, { overwrite = false } = {}) {
        const resolved = {
            from: await this._filename(from),
            to: await this._filename(to)
        }
        if (resolved.to && resolved.to.staged) {
            if (!overwrite) {
                this._error('EEXISTS', to)
            }
            fs.unlink(resolved.to.filename, { recursive: true })
        }
        if (resolved.from.staged) {
            await fs.mkdir(path.dirname(from), { recursive: true })
            // TODO How do I update the 'emplace' or rename?
            const temporary = {
                from: path.join(this.directory, resolved.from.relative),
                to: path.join(this._tmp.path, to)
            }
            await fs.rename(temporary.from, temporary.to)
            resolved.from.operation.filename = path.join(this._tmp.directory, to)
        } else {
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
            return false
        }
        const operations = await this._load(commit)
        // Start by deleting the commit script, once this runs we have to move
        // forward through the entire commit.
        await this._prepare([ 'begin' ])
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
                    await this._prepare([ 'rename', path.join('commit', file), hash ])
                }
                break
            case 'emplace': {
                    const { page, hash, filename, options } = operation
                    if (options.flag == 'w') {
                        await this._prepare([ 'unlink', filename ])
                    }
                    await this._prepare([ 'rename', filename, hash ])
                }
                break
            case 'unlink': {
                    await this._prepare([ 'unlink', operation.path ])
                }
                break
            }
        }
        await this._prepare([ 'end' ])
        return true
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
        for (const step of steps) {
            const operation = (await this._load(step.file)).shift()
            switch (operation.shift()) {
            case 'begin':
                const commit = dir.filter(function (file) {
                    return /^commit\./.test(file)
                }).shift()
                await fs.unlink(this._path(commit))
                break
            case 'rename': {
                    const filename = operation.shift()
                    const from = path.join(this._absolute.staging, filename)
                    const to = path.join(this.directory, filename)
                    await fs.mkdir(path.dirname(to), { recursive: true })
                    // When replayed from failure we'll get `ENOENT`.
                    await fs.rename(from, to)
                    const hash = operation.shift()
                    if (hash != null) {
                        const buffer = await fs.readFile(to)
                        // TODO Is there a suitable UNIX exception?
                        Journalist.Error.assert(hash == fnv(buffer), 'rename failed')
                    }
                }
                break
            case 'unlink':
                await this._unlink(path.join(this.directory, operation.shift()))
                break
            case 'end':
                break
            }
            await fs.unlink(this._path(step.file))
        }
    }

    async dispose () {
        await fs.rmdir(this._absolute.tmp, { recursive: true })
    }
}

exports.create = async function (directory, { tmp = 'commit', prepare = [] } = {}) {
    const journalist = new Journalist(directory, { tmp, prepare })
    await journalist._create()
    return journalist
}
