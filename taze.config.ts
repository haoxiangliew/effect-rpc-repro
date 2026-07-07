import { defineConfig } from "taze";

export default defineConfig({
  mode: "default",
  interactive: true,
  recursive: true,
  includeLocked: true,
  packageMode: {
    "/.*/": "major",
  },
});
