// ESLint 9 flat config. Scope: the frontend React app.
//
// Rule set is deliberately narrow -- only rules whose findings are ~always
// real bugs, no stylistic preferences:
//   * @eslint/js recommended -- pyflakes-equivalent basics (no-undef,
//     no-unused-vars, no-unreachable, etc.).
//   * eslint-plugin-react-hooks recommended -- rules-of-hooks and
//     exhaustive-deps; hook-dep issues are a common React bug source.
//   * eslint-plugin-react runtime rules only -- disables the older
//     react/react-in-jsx-scope check (Vite's new JSX transform handles it)
//     and skips stylistic rules.
//
// Formatting is out of scope for this config; Prettier can slot in later.

import js from "@eslint/js";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import globals from "globals";

export default [
  {
    // Ignore build output and any local dev directories.
    ignores: ["dist", "node_modules", "public"],
  },
  js.configs.recommended,
  {
    files: ["src/**/*.{js,jsx}"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
      globals: {
        ...globals.browser,
      },
    },
    settings: {
      react: { version: "detect" },
    },
    plugins: {
      react,
      "react-hooks": reactHooks,
    },
    rules: {
      // React runtime correctness.
      ...react.configs.recommended.rules,
      ...reactHooks.configs.recommended.rules,
      // Vite's new JSX transform imports React automatically.
      "react/react-in-jsx-scope": "off",
      // PropTypes are not used in this codebase and are optional in modern
      // React; leaving this on would produce noise on every component.
      "react/prop-types": "off",
    },
  },
  {
    // Vite config runs in Node, not the browser -- give it Node globals so
    // `process` and friends do not trigger no-undef.
    files: ["vite.config.js"],
    languageOptions: {
      globals: {
        ...globals.node,
      },
    },
  },
];
