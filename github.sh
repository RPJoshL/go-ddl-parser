#!/bin/bash
set -euo pipefail

SRC_DIR="$(pwd)"
DEST_DIR="$(dirname "$SRC_DIR")/go-ddl-parser-github"

rsync -a --delete --exclude='.git' --exclude='go.sum' "$SRC_DIR/" "$DEST_DIR/"

# This module
search_term="git.rpjosh.de"
search_term="$search_term/RPJosh/go-ddl-parser"
grep -Ilr "$search_term" $DEST_DIR | while IFS= read -r file; do
	sed -i "s|$search_term|github.com/RPJoshL/go-ddl-parser|g" "$file" || true
done

# Logger dependency
search_term="git.rpjosh.de"
search_term="$search_term/RPJosh/go-logger"
grep -Ilr "$search_term" $DEST_DIR | while IFS= read -r file; do
	sed -i "s|$search_term|github.com/RPJoshL/go-logger|g" "$file" || true
done

# Get new (correct) hash
cd "$DEST_DIR"
go mod tidy