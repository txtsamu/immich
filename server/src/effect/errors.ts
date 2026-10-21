import { Data } from 'effect';

/** A query or driver failure; `cause` is the original thrown value so adapters can rethrow it unchanged. */
export class DatabaseError extends Data.TaggedError('DatabaseError')<{ cause: unknown }> {}

/** A filesystem failure; `cause` is the original thrown value so adapters can rethrow it unchanged. */
export class FileSystemError extends Data.TaggedError('FileSystemError')<{ cause: unknown }> {}

/** A failure from a third-party library call (crypto, jwt, bcrypt, ...). */
export class LibraryError extends Data.TaggedError('LibraryError')<{ cause: unknown }> {}

/** Invalid or unreadable environment configuration; `cause` is the original thrown Error. */
export class ConfigError extends Data.TaggedError('ConfigError')<{ cause: unknown }> {}

/** A failure thrown by a registered event/job handler or other caller-supplied callback; `cause` is the original thrown value. */
export class HandlerError extends Data.TaggedError('HandlerError')<{ cause: unknown }> {}
