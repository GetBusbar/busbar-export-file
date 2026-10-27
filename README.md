<!-- fleet:header:begin (rendered by `busbar-release plugin heal` from GetBusbar/busbar-release template/ and busbar's plugins.yaml; edit it there) -->
# busbar-export-file

First-party signed kind:export plugin cdylib: the request-log FILE sink (module: request-log-file), packaged as a droppable busbar plugin. Drop the signed tarball into plugins/ and name it from an export.<name>.module: request-log-file block.

| kind | alias | crate | busbar | license |
|---|---|---|---|---|
| `export` | `request-log-file` | `busbar-export-file-plugin` | 1.6.0 (pinned in `.busbar-ref`) | Apache-2.0 |

[![ci](https://github.com/GetBusbar/busbar-export-file/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/GetBusbar/busbar-export-file/actions/workflows/ci.yml)
<!-- fleet:header:end -->

## What it is for

`busbar-export-file` is a `kind: export` busbar plugin.

## Config

Configured from an `export.<name>.module: request-log-file` block. Its `settings:` are:

- `path` (required): the JSONL file each request-log line is appended to.
- `rotate_mb` (optional, MiB): the size at which the file is rotated; absent means never rotate.
- `keep` (optional, default `9`): how many archives a rotation keeps. The file is renamed to `<path>.1`, older archives shift up to `<path>.9`, and only the oldest is dropped. `keep: 0` truncates the file in place and keeps no archive, as 1.5.5 did. The host applies it (1.6.0 changed the default from 1.5.5's truncation; owner sign-off 2026-10-27, "Keep N, default 9").

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
