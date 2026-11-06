<!-- fleet:header:begin (rendered by `busbar-release plugin heal` from GetBusbar/busbar-release template/ and busbar's plugins.yaml; edit it there) -->
# busbar-export-file

First-party signed kind:export plugin cdylib: the request-log FILE sink (module: request-log-file), packaged as a droppable busbar plugin. Drop the signed tarball into plugins/ and name it from an export.<name>.module: request-log-file block.

| kind | alias | crate | busbar | license |
|---|---|---|---|---|
| `export` | `request-log-file` | `busbar-export-file-plugin` | 1.6.0 (pinned in `.busbar-ref`) | Apache-2.0 |

<!-- fleet:header:end -->

## What it is for

`busbar-export-file` appends request-log batches unchanged to a JSONL file through the host's
`disk.append` service. The host opens and rotates the file; the sink never opens a path itself.
Append and open failures drop the batch and report BUSBAR-7073 or BUSBAR-7074. Rotation failures
warn under BUSBAR-7075 through BUSBAR-7077.

## Config

Configured from an `export.<name>.module: request-log-file` block. Its `settings:` are:

- `path` (required): the JSONL file each request-log line is appended to.
- `rotate_mb` (optional, MiB): the size at which the file is rotated; absent means never rotate.
- `keep` (optional, default `9`): how many archives a rotation keeps. When `rotate_mb` is set, the file is renamed to `<path>.1`, older archives shift up to `<path>.<keep>`, and only the oldest is dropped. Without `rotate_mb`, `keep` has no effect. `keep` is at most `64`; a larger value is refused. `keep: 0` truncates the file in place and keeps no archive, as 1.5.5 did. The host applies it (1.6.0 changed the default from 1.5.5's truncation; owner sign-off 2026-10-27, "Keep N, default 9").

`rotate_mb: 0` is accepted, as 1.5.5 accepted it: the file is rotated before every append. An instance opened or reloaded with it logs a warning saying so.

## Differences from 1.5.5

- An empty `path`, or one containing a NUL character, is refused when the settings are validated (``settings: `path` must name a file`` / ``settings: `path` must not contain a NUL character``). 1.5.5 accepted such a path and then dropped every request-log line with an open failure; no file can ever be opened at such a path, so the sink now refuses it at boot or reload instead of losing every line at runtime.

## Build

```bash
cargo build --release -p busbar-export-file-plugin --features dropped-in
```

## Tests

```bash
cargo test --workspace --locked
```

## License

Apache-2.0. See [LICENSE](LICENSE).
