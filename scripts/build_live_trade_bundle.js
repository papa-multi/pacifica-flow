const fs = require("fs");
const path = require("path");

let babel;
let presetEnv;
try {
  babel = require("@babel/core");
  presetEnv = require("@babel/preset-env");
} catch (error) {
  console.error(
    "[build_live_trade_bundle] Missing Babel runtime. Install @babel/core and @babel/preset-env to rebuild the browser bundle."
  );
  process.exit(1);
}

const ROOT = path.resolve(__dirname, "..");
const srcPath = path.join(ROOT, "public", "live-trade", "app.src.js");
const outPath = path.join(ROOT, "public", "live-trade", "app.js");

const source = fs.readFileSync(srcPath, "utf8");
const result = babel.transformSync(source, {
  babelrc: false,
  configFile: false,
  comments: false,
  compact: true,
  minified: true,
  presets: [
    [
      presetEnv,
      {
        targets: {
          chrome: "70",
          firefox: "68",
          safari: "13",
          edge: "79",
        },
        bugfixes: true,
      },
    ],
  ],
});

fs.writeFileSync(outPath, `${result.code}\n`, "utf8");
console.log(`[build_live_trade_bundle] wrote ${path.relative(ROOT, outPath)}`);
