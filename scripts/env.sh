#!/usr/bin/env bash
# Assessment environment — isolated CE profile only.
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"
export DATABRICKS_HOST="${DATABRICKS_HOST:-https://dbc-06f970f4-0f19.cloud.databricks.com}"

# GitHub MCP token for project-level `.cursor/mcp.json` (JGirulkar only).
# Cursor resolves ${env:GITHUB_DE_ASSESSMENT_TOKEN} when the IDE inherits this shell.
if [[ -z "${GITHUB_DE_ASSESSMENT_TOKEN:-}" ]] && command -v gh >/dev/null 2>&1; then
  if gh auth status -u JGirulkar >/dev/null 2>&1; then
    export GITHUB_DE_ASSESSMENT_TOKEN="$(gh auth token -u JGirulkar)"
  fi
fi

# JDK 21 for local Spark (adjust path if needed)
if [[ -d /usr/lib/jvm/java-21-openjdk-amd64 ]]; then
  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
elif [[ -d /usr/lib/jvm/java-21-openjdk ]]; then
  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
fi

export PATH="${HOME}/.databricks/bin:${HOME}/.local/bin:${PATH}"
