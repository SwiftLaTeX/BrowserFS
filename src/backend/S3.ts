import {BFSOneArgCallback, BFSCallback, FileSystemOptions} from '../core/file_system';
import {AsyncKeyValueROTransaction, AsyncKeyValueRWTransaction, AsyncKeyValueStore, AsyncKeyValueFileSystem} from '../generic/key_value_filesystem';
import {ApiError, ErrorCode} from '../core/api_error';
import {arrayBuffer2Buffer} from '../core/util';
import * as AWS from "aws-sdk";

/**
 * Converts a DOMException or a DOMError from an IndexedDB event into a
 * standardized BrowserFS API error.
 * @hidden
 */
function convertError(e: {name: string}, message: string = e.toString()): ApiError {
  // switch (e.name) {
  //   case "NotFoundError":
  //     return new ApiError(ErrorCode.ENOENT, message);
  //   case "QuotaExceededError":
  //     return new ApiError(ErrorCode.ENOSPC, message);
  //   default:
      // The rest do not seem to map cleanly to standard error codes.
      return new ApiError(ErrorCode.EIO, message);
  // }
}

/**
 * Produces a new onerror handler for IDB. Our errors are always fatal, so we
 * handle them generically: Call the user-supplied callback with a translated
 * version of the error, and let the error bubble up.
 * @hidden
 */
function onErrorHandler(cb: (e: ApiError) => void, code: ErrorCode = ErrorCode.EIO, message: string | null = null): (e?: any) => void {
  return function(e?: any): void {
    // Prevent the error from canceling the transaction.
    e.preventDefault();
    cb(new ApiError(code, message !== null ? message : undefined));
  };
}

/**
 * It is a fix for minio as minio is not real key/value storage as S3, so no support for "/"
 * @hidden
 */
function sanitizeKey(key: string) {
  if(key.endsWith("/")) {
    const DELIMITER = "_DiR_mImiC_";
    return key.substring(0, key.length - 1) + DELIMITER;
  } else {
    return key;
  }
}

/**
 * @hidden
 */
export class S3ROTransaction implements AsyncKeyValueROTransaction {
  constructor(protected db: AWS.S3, protected s3opts: S3FileSystemOptions) { }

  public get(key: string, cb: BFSCallback<Buffer>): void {
    try {
      this.db.getObject({
        Bucket: this.s3opts.storeName,
        Key: this.s3opts.prefix + sanitizeKey(key),
      }, function(err: any, data: any) {
        if (err) {
          if (err.code === 'NoSuchKey') {
            cb(null, undefined);
          } else {
            onErrorHandler(cb);
          }
        } else {
          cb(null, arrayBuffer2Buffer(data.Body));
        }
      });
    } catch (e) {
      cb(convertError(e));
    }
  }
}

/**
 * @hidden
 */
export class S3RWTransaction extends S3ROTransaction implements AsyncKeyValueRWTransaction, AsyncKeyValueROTransaction {
  constructor(db: AWS.S3, s3opts: S3FileSystemOptions) {
    super(db, s3opts);
  }

  public put(key: string, data: Buffer, overwrite: boolean, cb: BFSCallback<boolean>): void {
    try {
      if (!overwrite) {
        // console.log("S3 backend will always overwrite files.");
      }
      this.db.upload({
      Bucket: this.s3opts.storeName,
      Key: this.s3opts.prefix + sanitizeKey(key),
      Body: data,
      }, function(err: any) {
        if (err) {
          onErrorHandler(cb);
        } else {
          cb(null, true);
        }
      });
    } catch (e) {
      cb(convertError(e));
    }
  }

  public del(key: string, cb: BFSOneArgCallback): void {
    try {
      this.db.upload({
      Bucket: this.s3opts.storeName,
      Key: this.s3opts.prefix + sanitizeKey(key),
      }, function(err: any) {
        if (err) {
          onErrorHandler(cb);
        } else {
          cb();
        }
      });
    } catch (e) {
      cb(convertError(e));
    }
  }

  public commit(cb: BFSOneArgCallback): void {
    // Return to the event loop to commit the transaction.
    cb();
  }

  public abort(cb: BFSOneArgCallback): void {
    const _e: ApiError | null = null;
    cb(_e);
  }
}

export class S3Store implements AsyncKeyValueStore {
  public static Create(opts: S3FileSystemOptions, cb: BFSCallback<S3Store>): void {
    const s3 = new AWS.S3({
      apiVersion: '2006-03-01',
      accessKeyId: opts.apiKey,
      secretAccessKey: opts.apiSecret,
      endpoint: opts.endpoint,
      s3ForcePathStyle: true,
      signatureVersion: 'v4'
    });
    // Preflight test
    s3.putObject({
      Bucket: opts.storeName,
      Key: opts.prefix + 'swiftlatex.txt',
      Body: 'Hello world',
    }, function(err: any, data: any) {
      if (err) {
        onErrorHandler(cb, ErrorCode.EACCES);
      } else {
        cb(null, new S3Store(s3, opts));
      }
    });
  }

  constructor(private db: AWS.S3, private s3opts: S3FileSystemOptions) {

  }

  public name(): string {
    return S3FileSystem.Name + ' - ' + this.s3opts.storeName + ' - ' + this.s3opts.prefix;
  }

  public clear(cb: BFSOneArgCallback): void {
    // console.warn("S3 refuses to clear a database by design");
    setTimeout(cb, 0);
  }

  public beginTransaction(type: 'readonly'): AsyncKeyValueROTransaction;
  public beginTransaction(type: 'readwrite'): AsyncKeyValueRWTransaction;
  public beginTransaction(type: 'readonly' | 'readwrite' = 'readonly'): AsyncKeyValueROTransaction {
    if (type === 'readwrite') {
      return new S3RWTransaction(this.db, this.s3opts);
    } else if (type === 'readonly') {
      return new S3ROTransaction(this.db, this.s3opts);
    } else {
      throw new ApiError(ErrorCode.EINVAL, 'Invalid transaction type.');
    }
  }
}

/**
 * Configuration options for the IndexedDB file system.
 */
export interface S3FileSystemOptions {
  // The name of this file system. You can have multiple IndexedDB file systems operating
  // at once, but each must have a different name.
  storeName: string;
  prefix: string;
  // The Key, Secret, Token
  apiKey: string;
  apiSecret: string;
  sessionToken: string;
  endpoint: string;
  // The size of the inode cache. Defaults to 100. A size of 0 or below disables caching.
  cacheSize?: number;
}

/**
 * A file system that uses the IndexedDB key value file system.
 */
export default class S3FileSystem extends AsyncKeyValueFileSystem {
  public static readonly Name = "S3";

  public static readonly Options: FileSystemOptions = {
    storeName: {
      type: "string",
      optional: false,
      description: "bucket of the S3 file system."
    },
    prefix: {
      type: "string",
      optional: false,
      description: "prefix of the S3 file system."
    },
    apiKey: {
      type: "string",
      optional: false,
      description: "API key."
    },
    apiSecret: {
      type: "string",
      optional: false,
      description: "API secret."
    },
    sessionToken: {
      type: "string",
      optional: false,
      description: "Session token."
    },
    endpoint: {
      type: "string",
      optional: false,
      description: "Endpoint url."
    },
    cacheSize: {
      type: "number",
      optional: true,
      description: "The size of the inode cache. Defaults to 100. A size of 0 or below disables caching."
    }
  };

  /**
   * Constructs an IndexedDB file system with the given options.
   */
  public static Create(opts: S3FileSystemOptions, cb: BFSCallback<S3FileSystem>): void {
    S3Store.Create(opts, (e, store?) => {
      if (store) {
        const s3fs = new S3FileSystem(typeof(opts.cacheSize) === 'number' ? opts.cacheSize : 1024);
        s3fs.init(store, (e) => {
          if (e) {
            cb(e);
          } else {
            cb(null, s3fs);
          }
        });
      } else {
        cb(e);
      }
    });
  }
  public static isAvailable(): boolean {
    return true;
  }
  private constructor(cacheSize: number) {
    super(cacheSize);
  }
}
