/** @jsxImportSource @opentui/solid */
// OpenCode 2 TUI plugin: lists the session's project jobs in the sidebar.
// Job data comes from the server plugin over SchedulerRpc, so this also works
// when the TUI is attached to a remote OpenCode service.
//
// Shipped precompiled as dist/tui.js (scripts/build-tui.ts). Keep runtime
// imports to solid-js and @opentui/*: the build points those at the host's
// runtime, and nothing else is installed next to the plugin.
import type { Plugin } from "@opencode/plugin/tui"
import type { Context } from "@opencode/plugin/tui/context"
import { createEffect, createSignal, For, Show } from "solid-js"
import { createStore, reconcile } from "solid-js/store"
import { SchedulerRpc, type JobListOutput, type JobState, type JobSummary } from "./rpc"

// Keeps relative times current and recovers from events missed while the
// event stream was disconnected.
const REFRESH_MS = 30_000

const plugin: Plugin.Definition = {
  id: "opencode-scheduler.tui",
  setup(context) {
    const rpc = context.client.rpc(SchedulerRpc)
    const [jobs, setJobs] = createStore<Record<string, JobSummary[]>>({})
    const [now, setNow] = createSignal(Date.now())
    const tracked = new Set<string>()
    const inflight = new Set<string>()
    const pending = new Set<string>()

    // Durable and live-synced across running TUIs.
    const [view, setView] = context.storage.store("sidebar", { initial: { collapsed: false } })
    const toggleCollapsed = () => {
      void setView((draft) => {
        draft.collapsed = !draft.collapsed
      }).catch(() => {})
    }

    const refresh = async (directory: string): Promise<void> => {
      if (inflight.has(directory)) {
        pending.add(directory)
        return
      }
      inflight.add(directory)
      try {
        const output = (await rpc.list({}, { location: { directory } })) as JobListOutput
        setJobs(directory, reconcile(output.jobs, { key: "slug" }))
      } catch {
        // Server plugin not loaded at this location, or the service is
        // unreachable: keep showing the last known list.
      } finally {
        inflight.delete(directory)
        if (pending.delete(directory)) void refresh(directory)
      }
    }

    const track = (directory: string) => {
      if (tracked.has(directory)) return
      tracked.add(directory)
      void refresh(directory)
    }

    const stopEvents = rpc.events.on("changed", (event) => {
      const directory = event.location.directory
      if (tracked.has(directory)) void refresh(directory)
    })

    const timer = setInterval(() => {
      setNow(Date.now())
      for (const directory of tracked) void refresh(directory)
    }, REFRESH_MS)

    const removeSlot = context.ui.slot({
      append: "sidebar.content",
      render: (input) => {
        const directory = () =>
          context.data.session.get(input.sessionID)?.location.directory ??
          context.location?.directory ??
          context.data.location.default().directory
        createEffect(() => track(directory()))
        return (
          <JobsSection
            context={context}
            jobs={jobs[directory()] ?? []}
            now={now()}
            collapsed={view.collapsed === true}
            onToggle={toggleCollapsed}
          />
        )
      },
    })

    // `app` stays mounted for the whole TUI session, so the command is
    // reachable from every route.
    const removeCommands = context.ui.slot({
      append: "app",
      render: () => <Commands context={context} toggle={toggleCollapsed} />,
    })

    return () => {
      stopEvents()
      clearInterval(timer)
      removeSlot()
      removeCommands()
    }
  },
}

export default plugin

// keymap.layer must run inside the host's component tree, not in setup(),
// so this component exists only to own the layer.
function Commands(props: { context: Context; toggle: () => void }) {
  props.context.keymap.layer(() => ({
    // The palette runs as a dialog, which leaves the base input mode.
    mode: "global",
    commands: [
      {
        id: "opencode-scheduler.sidebar.toggle",
        title: "Scheduler: Toggle sidebar jobs",
        description: "Collapse or expand the scheduled jobs section in the sidebar",
        group: "Scheduler",
        palette: true,
        run: () => props.toggle(),
      },
    ],
    bindings: [],
  }))
  return null
}

const ALERT_STATES = ["running", "failed", "stale"] as const

function JobsSection(props: {
  context: Context
  jobs: JobSummary[]
  now: number
  collapsed: boolean
  onToggle: () => void
}) {
  const theme = () => props.context.theme
  const stateColor = (state: JobState) => {
    switch (state) {
      case "running":
        return theme().text.feedback.info.base
      case "success":
        return theme().text.feedback.success.base
      case "failed":
        return theme().text.feedback.error.base
      case "stale":
        return theme().text.feedback.warning.base
      case "never":
        return theme().text.muted
    }
  }

  // Collapsed, the header still surfaces anything that needs attention.
  const alerts = () =>
    ALERT_STATES.map((state) => ({ state, count: props.jobs.filter((job) => job.state === state).length })).filter(
      (alert) => alert.count > 0
    )

  return (
    <Show when={props.jobs.length > 0}>
      <box flexDirection="column">
        <box flexDirection="row" onMouseDown={() => props.onToggle()}>
          <text fg={theme().text.base} wrapMode="none">
            <b>{props.collapsed ? "▶" : "▼"} Scheduled jobs</b>
          </text>
          <text fg={theme().text.muted} wrapMode="none">
            {` (${props.jobs.length})`}
          </text>
          <Show when={props.collapsed}>
            <For each={alerts()}>
              {(alert) => (
                <text fg={stateColor(alert.state)} wrapMode="none">
                  {`  ${STATE_ICON[alert.state]} ${alert.count}`}
                </text>
              )}
            </For>
          </Show>
        </box>
        <For each={props.collapsed ? [] : props.jobs}>
          {(job) => (
            <box flexDirection="column">
              <box flexDirection="row">
                <text fg={stateColor(job.state)}>{STATE_ICON[job.state]} </text>
                <text fg={theme().text.base} wrapMode="none" truncate>
                  {job.name}
                </text>
              </box>
              <text fg={theme().text.muted} wrapMode="none" truncate>
                {"  "}
                {describeJob(job, props.now)}
              </text>
            </box>
          )}
        </For>
      </box>
    </Show>
  )
}

const STATE_ICON: Record<JobState, string> = {
  running: "●",
  success: "✓",
  failed: "✗",
  stale: "!",
  never: "○",
}

function describeJob(job: JobSummary, now: number): string {
  const parts: string[] = []
  if (job.state === "running") {
    parts.push(job.lastRunAt ? `running ${elapsed(job.lastRunAt, now)}` : "running")
  } else if (job.nextRunAt) {
    parts.push(`next ${until(job.nextRunAt, now)}`)
  } else {
    parts.push(job.scheduleText)
  }

  if (job.state === "never") {
    parts.push("never run")
  } else if (job.state === "failed") {
    const reason = job.lastRunExitCode !== undefined ? `exit ${job.lastRunExitCode}` : job.lastRunError ?? "failed"
    parts.push(job.lastRunAt ? `${reason} ${elapsed(job.lastRunAt, now)}` : reason)
  } else if (job.state === "stale") {
    parts.push(job.lastRunAt ? `interrupted ${elapsed(job.lastRunAt, now)}` : "interrupted")
  } else if (job.state === "success" && job.lastRunAt) {
    parts.push(`ok ${elapsed(job.lastRunAt, now)}`)
  }
  return parts.join(" · ")
}

function until(iso: string, now: number): string {
  const ms = Date.parse(iso) - now
  if (!Number.isFinite(ms)) return "?"
  return ms < 60_000 ? "<1m" : `in ${duration(ms)}`
}

function elapsed(iso: string, now: number): string {
  const ms = now - Date.parse(iso)
  if (!Number.isFinite(ms)) return ""
  return ms < 60_000 ? "just now" : `${duration(ms)} ago`
}

function duration(ms: number): string {
  const minutes = Math.floor(ms / 60_000)
  if (minutes < 60) return `${minutes}m`
  const hours = Math.floor(minutes / 60)
  if (hours < 48) return `${hours}h`
  return `${Math.floor(hours / 24)}d`
}
