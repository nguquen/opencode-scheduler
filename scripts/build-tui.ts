// Builds dist/tui.js, the OpenCode 2 TUI entry.
//
// OpenCode's Solid transform skips files under node_modules, so an installed
// package cannot ship raw TSX. Compile the JSX here and point solid-js and
// @opentui/* at the host's runtime registry, so the plugin shares the host's
// single Solid instance instead of needing its own copy (two copies render,
// but signals from one never update effects in the other).
import { readFile } from "node:fs/promises"
import { transformAsync } from "@babel/core"
// @ts-expect-error untyped
import typescript from "@babel/preset-typescript"
// @ts-expect-error untyped
import solid from "babel-preset-solid"

// Modules the host registers in its OpenTUI runtime registry.
const RUNTIME_SPECIFIERS = ["@opentui/core", "@opentui/solid", "solid-js", "solid-js/store"]
const RUNTIME_PREFIX = "opentui:runtime-module:"

const runtimeModuleId = (specifier: string) => RUNTIME_PREFIX + encodeURIComponent(specifier)

const SPECIFIER_PATTERN = new RegExp(
  `(\\bfrom\\s*|\\bimport\\s*\\(?\\s*)(["'])(${RUNTIME_SPECIFIERS.map((s) => s.replace(/[.*+?^${}()|[\]\\/]/g, "\\$&")).join("|")})\\2`,
  "g"
)

async function compile(path: string): Promise<string> {
  const result = await transformAsync(await readFile(path, "utf8"), {
    filename: path,
    configFile: false,
    babelrc: false,
    presets: [
      [solid, { moduleName: "@opentui/solid", generate: "universal" }],
      [typescript, { onlyRemoveTypeImports: false }],
    ],
  })
  if (!result?.code) throw new Error(`babel produced no output for ${path}`)
  return result.code.replace(
    SPECIFIER_PATTERN,
    (_match, lead: string, quote: string, specifier: string) => `${lead}${quote}${runtimeModuleId(specifier)}${quote}`
  )
}

const result = await Bun.build({
  entrypoints: ["src/tui.tsx"],
  outdir: "dist",
  target: "bun",
  format: "esm",
  external: [`${RUNTIME_PREFIX}*`],
  plugins: [
    {
      name: "solid-host-runtime",
      setup(build) {
        build.onLoad({ filter: /[/\\]src[/\\].*\.tsx?$/ }, async (args) => ({
          contents: await compile(args.path),
          loader: "js",
        }))
      },
    },
  ],
})

if (!result.success) {
  for (const log of result.logs) console.error(log)
  process.exit(1)
}

// Anything else would have to be installed next to the plugin, and is not.
const output = await Bun.file("dist/tui.js").text()
const stray = [...output.matchAll(/^import\s[^;]*?from\s*"([^"]+)"/gm)]
  .map((match) => match[1])
  .filter((specifier) => !specifier.startsWith(RUNTIME_PREFIX))
if (stray.length > 0) {
  console.error(`dist/tui.js imports packages the host does not provide: ${stray.join(", ")}`)
  process.exit(1)
}
