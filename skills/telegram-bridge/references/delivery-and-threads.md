# Direct Delivery And Threads

Read this reference for OMP Workspace file delivery, or Pi adapter local/TUI delivery, cross-target delivery, or Thread routing.

## OMP Workspace Files

Include a Markdown link in the main final reply, such as `[design.md](<openspec/changes/example/design.md>)`. The gateway sends the file as a document to the current topic without a dedicated tool.

- Relative paths resolve from the session cwd; absolute paths must stay inside it after symlink resolution. `local://` links use the exact session's local artifact directory instead.
- Angle brackets support spaces and parentheses; percent-encoded paths also work. Bare paths do not attach anything.
- Only existing regular files are sent. Workspace links inside code examples are ignored; duplicate destinations upload once.
- Limit: 10 files per reply, 50 MB each. External URLs, missing files, directories, and files outside the permitted roots are not uploaded.
- Sidecar replies are text-only. OMP Workspace does not implement the Pi adapter's direct-delivery tools or action markup.

The remaining sections describe the Pi adapter.

## Direct Delivery

Use `telegram_message` only when the user explicitly requests Telegram delivery from local/TUI or names a concrete different Telegram target.

- Omitted target selects the paired/default target only outside an active Telegram turn.
- `chat_id` plus optional `thread_id` selects an explicit Bot API target. A public `@username`, or an exact negative numeric channel ID with `channel: true`, uses `chat_id` without `thread_id`; channel delivery requires the direct leader, and Telegram enforces the bot's posting permission.
- `thread` selects another live Pi Thread by case-insensitive name or numeric id and admits one attributed turn there.
- During an active Telegram turn, answer the current target normally; direct delivery to that same target is rejected.
- Direct delivery requires this Pi instance to own transport or hold a live Threaded Mode registration.
- Unknown, ambiguous, same, offline, unauthorized, or cross-chat targets fail closed.

Use `telegram_attach` outside Telegram turns only for an explicit file-delivery request. Registered followers default to their assigned Thread; explicit targets must preserve both `chat_id` and `thread_id`.

## Threaded Mode

Threaded Mode uses one leader transport and visible operator-started follower Pi processes.

- `Thread` is the product term; reserve `topic` for Bot API primitives.
- A Thread follows its assigned live Pi instance and current session.
- The `All` surface controls routing; it does not create processes.
- Never invent hidden followers, launch shadow Pi processes, expose internal bus roles as user identity, or rename Threads through guessed prompts or unsupported tools.

Cross-Thread delivery must preserve the concrete target and current registration authority. Keep the source turn on its ordinary reply path and use `telegram_message(thread=...)` only for an explicitly requested different live Thread.
