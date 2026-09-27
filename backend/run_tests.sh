#!/bin/sh
# Run every offline backend test under coverage; stop at the first failure.
# No .env, network or database needed. From backend/:
#     sh run_tests.sh
# Used by .github/workflows/ci.yml. Needs `coverage` installed, and a model in
# ml/ for test_unseen_merchants (CI trains a templates-only one first).
set -e
export PYTHONIOENCODING=utf-8
coverage erase
for t in test_gmail_parser test_merchant_identity test_model_features \
         test_api_security test_pipeline test_unseen_merchants test_export_demo test_weekly_retrain; do
    echo "== $t"
    coverage run --append "$t.py"
done
echo "ALL OFFLINE TESTS PASSED"
