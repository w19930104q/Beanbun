#!/usr/bin/env bash
# Beanbun transactional crawler regression suite.
# Usage: ./tests/run_tests.sh [php-binary]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PHP="${1:-php}"

cd "$ROOT"

echo "### PHP version"
"$PHP" -v | head -1
echo

for suite in \
    tests/LocalQueueTest.php \
    tests/RedisQueueTest.php \
    tests/MemoryQueueTest.php \
    tests/BeanbunTransactionTest.php \
    tests/MemoryQueueForkTest.php \
    tests/LegacyCompatibilityTest.php
do
    echo "### $suite"
    "$PHP" "$suite"
    echo
done

echo "All regression suites passed."
