/**
 * OpenCode 2 `opencode run` arguments, and the plugin-start migration of job
 * invocations stored in the old form.
 */
import { existsSync, readdirSync, readFileSync, renameSync, writeFileSync } from "fs"
import { join } from "path"

export type OpencodeRunFormat = "default" | "json"

export interface JobRunSpec {
  prompt?: string
  slashCommand?: string
  slashCommandArgs?: string

  files?: string[]
  agent?: string
  model?: string
  variant?: string
  title?: string
  share?: boolean
  continue?: boolean
  session?: string
  runFormat?: OpencodeRunFormat

  attachUrl?: string
  port?: number
}

// Whitespace runs, newlines included, collapse to one space: the prompt
// reaches `run` as one line.
function promptWords(run: JobRunSpec): string[] {
  return (run.prompt ?? "").split(/\s+/).filter(Boolean)
}

// The prompt goes to OpenCode 2 `run` as plain words without a `--` separator
// (see buildOpencodeV2Args), so a word starting with `-` and at least one more
// character would be parsed as a flag: `-c` silently becomes --continue, an
// unknown flag fails the run, a lone `--` brings back the doubled prompt.
export function promptFlagWord(prompt: string | undefined): string | undefined {
  return (prompt ?? "").split(/\s+/).find((word) => /^-./.test(word))
}

export function validateV2Prompt(prompt: string | undefined): void {
  const word = promptFlagWord(prompt)
  if (word === undefined) return
  throw new Error(
    `OpenCode 2 \`run\` would read the prompt word "${word}" as a flag. ` +
      `Reword it so no word starts with "-" (e.g. "minus 5", or put it after a letter); a lone "-" is fine.`
  )
}

// OpenCode 2 `run`: the directory comes from PWD/cwd (the supervisor and
// run_job set both to the job workdir), the server from --server or the
// background service, and the variant rides on the model as provider/model#variant.
// Questions are cancelled and permission asks rejected without --auto, which
// matches the OpenCode 1 `question: deny` policy, so no extra flag is needed.
function v2FlagArgs(run: JobRunSpec): string[] {
  const unsupported = [
    run.slashCommand && "slashCommand (OpenCode 2 `run` has no --command; put the instructions in prompt)",
    run.port !== undefined && "port (OpenCode 2 `run` has no --port)",
    run.share && "share (OpenCode 2 `run` has no --share)",
  ].filter(Boolean)
  if (unsupported.length) {
    throw new Error(`Not supported on OpenCode 2: ${unsupported.join("; ")}`)
  }
  if (run.variant && !run.model) {
    throw new Error("OpenCode 2 selects a variant through the model (provider/model#variant); set model too")
  }
  if (run.variant && run.model?.includes("#")) {
    throw new Error("Set the variant either in model (provider/model#variant) or in variant, not both")
  }

  const args = ["run"]
  if (run.attachUrl) args.push("--server", run.attachUrl)
  if (run.agent) args.push("--agent", run.agent)
  if (run.model) args.push("--model", run.variant ? `${run.model}#${run.variant}` : run.model)
  if (run.runFormat) args.push("--format", run.runFormat)
  if (run.title) args.push("--title", run.title)
  if (run.continue) args.push("--continue")
  if (run.session) args.push("--session", run.session)
  for (const file of run.files ?? []) args.push("--file", file)
  return args
}

// The prompt goes as separate words with no `--` before them. OpenCode 2 `run`
// (2.1.2) adds the words after `--` to its message a second time, and wraps any
// argument containing a space in literal quotes, so separate words without
// `--` are the only form that arrives exactly.
export function buildOpencodeV2Args(run: JobRunSpec): string[] {
  const flags = v2FlagArgs(run)
  validateV2Prompt(run.prompt)
  return [...flags, ...promptWords(run)]
}

// What plugin versions up to 1.6.3 stored for the same run spec.
export function legacyOpencodeV2Args(run: JobRunSpec): string[] {
  return [...v2FlagArgs(run), "--", ...promptWords(run)]
}

export interface InvocationMigration {
  migrated: string[]
  refused: string[]
}

function sameArgs(a: unknown[], b: string[]): boolean {
  return a.length === b.length && a.every((value, i) => value === b[i])
}

// Rewrites the stored invocation of OpenCode 2 jobs created before the `--`
// fix. A job is touched only when its args exactly equal the old form built
// from its own run spec, so OpenCode 1 jobs and hand-edited invocations stay
// as they are, and a second pass changes nothing. `runOf` returns the run
// spec the invocation builder sees for a parsed job file.
export function migrateV2Invocations(
  scopesDir: string,
  runOf: (raw: unknown) => JobRunSpec,
  warn: (message: string) => void = (message) => console.warn(`[opencode-scheduler] ${message}`)
): InvocationMigration {
  const result: InvocationMigration = { migrated: [], refused: [] }
  if (!existsSync(scopesDir)) return result

  for (const scope of readdirSync(scopesDir, { withFileTypes: true })) {
    if (!scope.isDirectory()) continue
    const jobsDir = join(scopesDir, scope.name, "jobs")
    let files: string[]
    try {
      files = readdirSync(jobsDir).filter((file) => file.endsWith(".json"))
    } catch {
      continue
    }

    for (const file of files) {
      const path = join(jobsDir, file)
      let raw: Record<string, unknown>
      let run: JobRunSpec
      try {
        raw = JSON.parse(readFileSync(path, "utf-8"))
        run = runOf(raw)
      } catch {
        continue
      }
      const invocation = raw.invocation as { command?: unknown; args?: unknown } | undefined
      if (!invocation || typeof invocation.command !== "string" || !Array.isArray(invocation.args)) continue

      let legacy: string[]
      try {
        legacy = legacyOpencodeV2Args(run)
      } catch {
        continue
      }
      if (!sameArgs(invocation.args, legacy)) continue

      let args: string[]
      try {
        args = buildOpencodeV2Args(run)
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        warn(`job ${scope.name}/${file} keeps its old invocation, which sends the prompt twice: ${message}`)
        result.refused.push(path)
        continue
      }

      raw.invocation = { ...invocation, args }
      // Atomic: supervisor.pl reads the file while a run is in progress.
      const tmp = `${path}.tmp.${process.pid}`
      writeFileSync(tmp, JSON.stringify(raw, null, 2))
      renameSync(tmp, path)
      result.migrated.push(path)
    }
  }
  return result
}
