import { defineConfig } from "vite";

export default defineConfig({
  server: {
    port: 8080,
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "credentialless",
    },
  },
  build: {
    target: "esnext",
  },
  optimizeDeps: {
    exclude: ["@duckdb/duckdb-wasm"],
  },
  // Ensure WASM files are served with correct MIME type
  assetsInclude: ["**/*.wasm"],
});
