#!/bin/bash

# Source secrets
. ./scripts/secrets.sh

# Run tests
go run "$1"