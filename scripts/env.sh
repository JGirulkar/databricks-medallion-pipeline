#!/usr/bin/env bash
# Assessment environment — isolated from Intelo.
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-de-assessment-ce}"

# JDK 21 for local Spark (adjust path if needed)
if [[ -d /usr/lib/jvm/java-21-openjdk-amd64 ]]; then
  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
elif [[ -d /usr/lib/jvm/java-21-openjdk ]]; then
  export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
fi

export PATH="${HOME}/.databricks/bin:${HOME}/.local/bin:${PATH}"
