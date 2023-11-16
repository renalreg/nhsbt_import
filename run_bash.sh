#!/bin/bash

COMMIT_MESSAGES=$(git log remotes/origin/main..pr-action --oneline --pretty=format:"%s" | grep -E '^(feat!|feat|fix|docs)')

SUPERIOR_COMMIT=$(echo "$COMMIT_MESSAGES" | grep -E '^(feat!)' | sort -r | head -n 1)

if [ -z "$SUPERIOR_COMMIT" ]; then
    SUPERIOR_COMMIT=$(echo "$COMMIT_MESSAGES" | grep -E '^(feat)' | sort -r | head -n 1)
fi

if [ -z "$SUPERIOR_COMMIT" ]; then
    SUPERIOR_COMMIT=$(echo "$COMMIT_MESSAGES" | grep -E '^(fix)' | sort -r | head -n 1)
fi

if [ -z "$SUPERIOR_COMMIT" ]; then
    SUPERIOR_COMMIT=$(echo "$COMMIT_MESSAGES" | grep -E '^(docs)' | sort -r | head -n 1)
fi

COUNT_SUPERIOR=$(echo "$COMMIT_MESSAGES" | grep -c "$SUPERIOR_COMMIT")

if [ "${COUNT_SUPERIOR}" -gt 1 ]; then
    echo "Exiting workflow. Two or more instances of the greatest commit found."
    exit 0
fi

COMMIT_BODIES=$(git log remotes/origin/main..pr-action --pretty=format:"%s%n%b" | awk '/^(feat!|feat|fix|docs)/ {inBody=1; next} inBody {print $0; next} {inBody=0}')
FEATURE_COMMITS=$(git log remotes/origin/main..pr-action --oneline --pretty=format:"%s" | grep -E '^(feat!|feat|fix|docs)')

echo this will be the pr title "${SUPERIOR_COMMIT}"

echo this will be the pr feature "${FEATURE_COMMITS}"

echo this will be the pr notes "${COMMIT_BODIES}"
