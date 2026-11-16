import { gzipSync } from "node:zlib";
import { build } from "esbuild";

const result = await build({
  entryPoints: ["src/index.ts"],
  bundle: true,
  format: "esm",
  minify: true,
  write: false,
  platform: "browser",
  target: "es2022",
});
const output = result.outputFiles[0];
const minified = output.contents.byteLength;
const gzipped = gzipSync(output.contents).byteLength;
console.log(`minified ${minified} bytes`);
console.log(`gzipped ${gzipped} bytes`);
