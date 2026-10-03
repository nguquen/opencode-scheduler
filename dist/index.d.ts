/**
 * OpenCode Scheduler Plugin
 *
 * Schedule recurring jobs using launchd (Mac), systemd (Linux), schtasks (Windows), or cron fallback.
 * Jobs are stored under ~/.config/opencode/scheduler/ (scoped by workdir).
 *
 * Features:
 * - Survives reboots
 * - Catches up on missed runs (if computer was asleep)
 * - Cross-platform (Mac + Linux + Windows)
 * - Working directory support for MCP configs
 * - Environment variable injection (PATH for node/npx)
 */
import type { Plugin } from "@opencode-ai/plugin";
import type { Plugin as PluginV2 } from "@opencode/plugin";
type PluginV2Context = PluginV2.Context;
declare function setupV2(ctx: PluginV2Context): Promise<() => Promise<void>>;
declare const _default: {
    id: string;
    server: Plugin;
    setup: typeof setupV2;
};
export default _default;
