# go-tool-cache

Do you like Go's built-in build & test caching but wish it weren't purely stored on local disk in the `$GOCACHE` directory?

Want to share your cache over the network between your various machines, coworkers, and CI runs without all that GitHub actions/caches tarring and untarring?

Go's [GOCACHEPROG](https://pkg.go.dev/cmd/go/internal/cacheprog) lets you do that!

This was a demonstration repro for when GOCACHEPROG was still a
[proposal](https://github.com/golang/go/issues/59719). Now it just contains some misc
examples.

## Status

GOCACHEPROG shipped as an experiment in Go 1.21. It became official in Go 1.24.

## Using

First, build your cache child process. For example,

```sh
$ go install github.com/bradfitz/go-tool-cache/cmd/go-cacher@latest
```

Then tell Go to use it:

```sh
$ GOCACHEPROG=$HOME/go/bin/go-cacher go install std
```

See some stats:

```sh
$ GOCACHEPROG="$HOME/go/bin/go-cacher --verbose" go install std
Defaulting to cache dir /home/bradfitz/.cache/go-cacher ...
cacher: closing; 548 gets (0 hits, 548 misses, 0 errors); 1090 puts (0 errors)
```

Run it again and watch the hit rate go up:

```sh
$ GOCACHEPROG="$HOME/go/bin/go-cacher --verbose" go install std
Defaulting to cache dir /home/bradfitz/.cache/go-cacher ...
cacher: closing; 808 gets (808 hits, 0 misses, 0 errors); 0 puts (0 errors)
```

## Shared local cache and sweeping

Multiple `go-cacher` helpers can safely use the same `--cache-dir`. On Unix,
each returned object path is protected by a shared advisory lock until that
helper receives the GOCACHEPROG `close` command or exits. Publishers and the
sweeper use the same stable sidecar lock files, so atomic object replacement
does not split coordination across object inodes. Output sidecar lock files are intentionally retained and are not counted as
cached object bytes. The `DiskCache` library defaults to transient validation;
the `go-cacher` GOCACHEPROG mode explicitly enables hold-open behavior.

Run one standalone sweeper per host (additional processes or pods sharing the
tree safely skip while another sweep is active):

```sh
go-cacher --sweep \
  --cache-dir=/var/cache/go-cacher \
  --max-size-gb=100 \
  --filesystem-high-percent=90 \
  --filesystem-low-percent=80 \
  --interval=10m \
  --jitter=5m \
  --initial-delay=5m
```

Sweep mode does not speak GOCACHEPROG. It waits for a randomized initial delay,
then periodically removes least-recently-used objects until both the object-byte
limit and filesystem low watermark are satisfied. Active objects are skipped,
and their action indexes are removed before object deletion. `SIGINT` and
`SIGTERM` stop the sweeper cleanly. Set `--max-size-gb=0` to disable the byte
limit, or set both filesystem percentages to zero to disable watermarks.
