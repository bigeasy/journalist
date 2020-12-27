// Node.js API.
const fs = require('fs').promises
const fileSystem = require('fs')
const crypto = require('crypto')
const path = require('path')
const os = require('os')
const util = require('util')
const assert = require('assert')

// Sensible `async`/`await` over Node.js streams.
const Staccato = require('staccato/redux')

// Mappings from `errno` to libuv error messages so we can duplicate them.
const errno = require('errno')

// Detailed exceptions with nested stack traces.
const Interrupt = require('interrupt')

// A non-cryptographic hash to assert the validity of the contents of a file.
const fnv = require('./fnv')

// Catch exceptions by type, message, property.
const rescue = require('rescue')
//

// Journalist is a utility for atomic file operations. I leverages the atomicity
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
    static Error = Interrupt.create('Journalist.Error', {
        EXISTING_COMMIT: 'commit directory not empty',
        INVALID_FLAG: 'invalid write file flag, only \'w\' and \'wx\' allowed',
        COMMIT_BAD_HASH: 'commit step hash validation failed',
        RENAME_BAD_HASH: 'emplaced or renamed file hash validation failed',
        RENAME_NOT_DIR: 'emplaced or renamed directory is not a directory',
        RENAME_NON_EXTANT: 'emplaced or renamed file or directory does not exist',
        NOT_A_DIRECTORY: 'destination path includes a part that is not a directory',
        PATH_DOES_NOT_EXIST: 'destination path does not exist'
    })

    static COMPOSING = Symbol('COMPOSING')
    static COMMITTING = Symbol('COMMITTING')

    constructor (directory, { tmp }) {
        this.state = Journalist.COMPOSING
        this.messages = []
        this.directory = path.normalize(directory)
        Journalist.Error.assert(path.isAbsolute(this.directory), 'DIRECTORY_PATH_NOT_ABSOLUTE')
        this._tmp = this._normalize(tmp)
        this.tmp = path.resolve(this.directory, this._tmp)
        Journalist.Error.assert(this.tmp.indexOf(this.directory) == 0, 'TMP_NOT_IN_DIRECTORY')
        this._operations = [{
            method: 'messages',
            normalized: null,
            messages: []
        }]
        this._script = null
    }

    static async create (directory, { tmp = 'tmp' } = {}) {
        const journalist = new Journalist(directory, { tmp })
        await journalist._recover()
        return journalist
    }

    _normalize (filename) {
        const normalized = path.normalize(filename)
        Journalist.Error.assert(!path.isAbsolute(normalized), 'PATH_NOT_RELATIVE')
        Journalist.Error.assert(!(
            ~normalized.split(path.sep).indexOf('..') ||
            normalized == '.'
        ), 'RELATIVE_PATH_EXITS_DIRECTORY')
        const primary = path.resolve(this.directory, normalized)
        Journalist.Error.assert(primary.indexOf(this.directory) == 0, 'PATH_NOT_IN_DIRECTORY')
        return normalized
    }

    message (message) {
        Journalist.Error.assert(this.state == Journalist.COMPOSING, 'ALREAY_COMMITTED')
        this._operations[0].messages.push(message)
    }

    unlink (filename) {
        Journalist.Error.assert(this.state == Journalist.COMPOSING, 'ALREAY_COMMITTED')
        const normalized = this._normalize(filename)
        this._operations.push({ method: 'unlink', normalized })
    }

    rmdir (filename) {
        Journalist.Error.assert(this.state == Journalist.COMPOSING, 'ALREAY_COMMITTED')
        const normalized = this._normalize(filename)
        this._operations.push({ method: 'rmdir', normalized })
    }
    //

    // Accepts a `mode` option which defaults to `0x777`.

    //
    mkdir (dirname, { mode = 0o777 } = {}) {
        Journalist.Error.assert(this.state == Journalist.COMPOSING, 'ALREAY_COMMITTED')
        const normalized = this._normalize(dirname)
        this._operations.push({ method: 'mkdir', normalized, mode })
    }

    rename (from, to) {
        Journalist.Error.assert(this.state == Journalist.COMPOSING, 'ALREAY_COMMITTED')
        const normalized = { from: this._normalize(from), to: this._normalize(to) }
        this._operations.push({ method: 'rename', normalized })
    }

    async prepare () {
        Journalist.Error.assert(this.state == Journalist.COMPOSING, 'ALREAY_COMMITTED')
        const dir = { directory: {}, exists: true, staged: false }

        function mk (filename, node) {
            const code = node.directory == null ? 'NOT_A_FILE' : 'NOT_A_DIRECTORY'
            const parts = filename.split(path.sep)
            const file = parts.pop()
            let iterator = dir
            for (let i = 0; i < parts.length; i++) {
                Journalist.Error.assert(iterator.directory != null, code)
                iterator = iterator.directory[parts[i]]
                Journalist.Error.assert(iterator != null && iterator.exists, 'PATH_DOES_NOT_EXIST', { filename })
            }
            const existing = iterator.directory[file]
            Journalist.Error.assert(existing == null || ! existing.exists, code)
            iterator.directory[file] = node
        }

        function rm (filename, type) {
            const parts = filename.split(path.sep)
            const file = parts.pop()
            let iterator = dir
            for (let i = 0; i < parts.length; i++) {
                Journalist.Error.assert(iterator.directory != null, 'NOT_A_DIRECTORY')
                iterator = iterator.directory[parts[i]]
            }
            const existing = iterator.directory[file]
            Journalist.Error.assert(existing != null && existing.exists, 'FILE_DOES_NOT_EXIST')
            switch (type) {
            case 'file':
                Journalist.Error.assert(existing.directory == null, 'NOT_A_FILE')
                brea
            case 'directory':
                Journalist.Error.assert(existing.directory != null, 'NOT_A_DIRECTORY')
                break
            }
            const node = iterator.directory[file]
            iterator.directory[file] = { directory: null, exists: false, staged: true }
            return node
        }

        const load = async (filename) => {
            const parts = filename.split(path.sep)
            let iterator = dir
            for (let i = 0, I = parts.length; iterator.exists && !iterator.staged && i < I; i++) {
                const part = parts[i]
                const child = iterator.directory[part]
                if (child == null) {
                    try {
                        const filename = path.join(this.directory, parts.slice(0, i + 1).join(path.sep))
                        const stat = await Journalist.Error.resolve(fs.stat(filename), 'IO_ERROR')
                        if (stat.isDirectory()) {
                            iterator = iterator.directory[part] = { directory: {}, exists: true, staged: false }
                        } else {
                            iterator = iterator.directory[part] = { directory: null, exists: true, staged: false  }
                        }
                    } catch (error) {
                        rescue(error, [{ code: 'ENOENT' }])
                        iterator = iterator.directory[part] = { directory: null, exists: false, staged: false }
                    }
                } else {
                    iterator = child
                }
            }
        }

        for (const operation of this._operations) {
            switch (operation.method) {
            case 'messages': {
                    if (operation.normalized != null) {
                        await load(operation.normalized)
                        rm(operation.normalized, 'file')
                    }
                }
                break
            case 'rmdir': {
                    await load(operation.normalized)
                    rm(operation.normalized, 'directory')
                }
                break
            case 'mkdir': {
                    await load(operation.normalized)
                    mk(operation.normalized, { directory: {}, exists: true, staged: true })
                }
                break
            case 'rename': {
                    await load(operation.normalized.from)
                    await load(operation.normalized.to)
                    mk(operation.normalized.to, rm(operation.normalized.from, null))
                }
                break
            case 'unlink': {
                    await load(operation.normalized)
                    rm(operation.normalized, 'file')
                }
                break
            }
        }

        const buffer = Buffer.from(this._operations.map(JSON.stringify).join('\n') + '\n')
        const commitfile = path.join(this.tmp, 'intermediate')
        await fs.writeFile(commitfile, buffer, { flags: 'as' })
        await fs.rename(commitfile, path.join(this.tmp, `commit.${fnv(buffer)}.0.pending`))
        await this._recover()
    }

    async _operate (operation) {
        async function _rescue (promise, pattern) {
            try {
                await promise
            } catch (error) {
                rescue(error, pattern)
            }
        }

        switch (operation.method) {
        case 'messages': {
                this.messages = operation.messages
                if (operation.normalized == null) {
                    break
                }
            }
            /* fall through */
        case 'unlink': {
                const absolute = path.join(this.directory, operation.normalized)
                await _rescue(Journalist.Error.resolve(fs.unlink(absolute), 'IO_ERROR'), [ 1, 1, { code: 'ENOENT' } ])
            }
            break
        case 'rename': {
                const { normalized } = operation
                const from = path.join(this.directory, normalized.from)
                const to = path.join(this.directory, normalized.to)
                await _rescue(Journalist.Error.resolve(fs.rename(from, to), 'IO_ERROR'), [ 1, 1, { code: 'ENOENT' } ])
            }
            break
        case 'mkdir': {
                const { normalized, mode } = operation
                const absolute = path.join(this.directory, normalized)
                await _rescue(Journalist.Error.resolve(fs.mkdir(absolute, { mode }), 'IO_ERROR'), [ 1, 1, { code: 'EEXIST' } ])
            }
            break
        case 'rmdir': {
                const { normalized } = operation
                const absolute = path.join(this.directory, normalized)
                await _rescue(Journalist.Error.resolve(fs.rmdir(absolute), 'IO_ERROR'), [ 1, 1, { code: 'ENOENT' } ])
            }
            break
        }
        return operation.method
    }

    async _advance (pending, index, length) {
        const from = path.join(this.tmp, [ 'commit' ].concat(pending).join('.'))
        if (index == length - 1) {
            pending[2] = 'complete'
        } else {
            pending[1]++
        }
        const to = path.join(this.tmp, [ 'commit' ].concat(pending).join('.'))
        await fs.rename(from, to)
    }

    async _load (parts) {
        const body = await fs.readFile(path.resolve(this.tmp, [ 'commit' ].concat(parts).join('.')))
        Journalist.Error.assert(fnv(body) == parts[0], 'COMMIT_CHECKSUM_INVALID')
        return body.toString().split('\n').slice(0, -1)
            .map(JSON.parse)
            .map((operation, index) => ({ operation, index }))
    }

    async _recover () {
        await fs.mkdir(this.tmp, { recursive: true })
        const dir = await fs.readdir(this.tmp)
        const isCommit = /^commit\.([0-9a-f]+)\.(\d+).(pending|complete)$/
        const commits = dir
            .map(file => isCommit.exec(file))
            .filter($ => $ != null)
            .map($ => [ $[1], +$[2], $[3] ])
        const completes = commits.filter(commit => commit[2] == 'complete')
        Journalist.Error.assert(completes.length < 2, 'MULTIPLE_COMPLETE_COMMITS')
        const complete = completes.shift()
        if (complete != null) {
            this._operations[0].normalized = path.join(this._tmp, [ 'commit' ].concat(complete).join('.'))
            const entries = await this._load(complete)
            this.messages = entries[0].operation.messages
        }
        const pendings = commits.filter(commit => commit[2] == 'pending')
        Journalist.Error.assert(pendings.length < 2, 'MULTIPLE_PENDING_COMMITS')
        const pending = pendings.shift()
        if (pending != null) {
            const entries = await this._load(pending)
            if (pending[1] != 0) {
                Journalist.Error.assert(complete == null, 'OVERLAPPING_COMMITS')
                this.messages = entries[0].operation.messages
            }
            this.state = Journalist.COMMITTING
            this._script = entries.slice(pending[1]).map(step => {
                return {
                    operate: () => this._operate(step.operation),
                    advance: () => this._advance(pending, step.index, entries.length)
                }
            })
        }
    }
    //

    // Exposed for the sake of being able to debug recovery.

    //
    *[Symbol.iterator] () {
        Journalist.Error.assert(this.state == Journalist.COMMITTING, 'UNPREPARED')
        while (this._script.length != 0) {
            yield this._script.shift()
        }
    }

    async commit () {
        for (const operation of this) {
            await operation.operate()
            await operation.advance()
        }
    }

    async dispose () {
        await fs.rmdir(this.tmp, { recursive: true })
    }
}

module.exports = Journalist
