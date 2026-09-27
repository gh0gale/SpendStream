#!/bin/sh
# Retrain the categoriser, gated on the golden set (plan Phase 4; replaces the
# weekly GitHub workflow, which could not work: training needs the user's
# private labelled data, which stays on this machine and out of git and the cloud).
#
#   1. compare run: templates + synthetic rows + 70% of 1year_data.csv (never a
#      golden-set merchant), scored on the golden set against the last accepted compare
#      run (ml/candidate_baseline_*). This is the honest number.
#   2. if it wins: ship run on everything (ml/data/private_ship.csv: all_data.csv
#      + 1year_data.csv, built by build_golden_set.py), written to
#      ml/candidate_ship_*. It has seen the golden set, so it cannot be scored.
#   3. you copy the ship files over ml/model_v2.pkl / label_encoder_v2.pkl and
#      restart the backend (the command is printed). Nothing is replaced for you.
#
# Run in the backend image from the repo root, read-write mount:
#   MSYS_NO_PATHCONV=1 docker run --rm --network none -v "$(pwd -W)/backend:/app" \
#       spendstream-backend sh retrain.sh
set -e
cd "$(dirname "$0")"
export PYTHONIOENCODING=utf-8

python build_golden_set.py
python train_model.py --no-smoke --extra-data ml/data/private_train.csv --out-prefix ml/candidate_compare

if [ -f ml/candidate_baseline_model.pkl ]; then
    python evaluate_model.py --model ml/candidate_compare_model.pkl --encoder ml/candidate_compare_encoder.pkl \
        --compare ml/candidate_baseline_model.pkl --compare-encoder ml/candidate_baseline_encoder.pkl
else
    echo "No accepted baseline yet (ml/candidate_baseline_*); scoring the candidate alone."
    python evaluate_model.py --model ml/candidate_compare_model.pkl --encoder ml/candidate_compare_encoder.pkl --thresholds
fi

cp ml/candidate_compare_model.pkl ml/candidate_baseline_model.pkl
cp ml/candidate_compare_encoder.pkl ml/candidate_baseline_encoder.pkl

python train_model.py --extra-data ml/data/private_ship.csv --out-prefix ml/candidate_ship
SHIP=ml/candidate_ship

echo
echo "To ship it (backend/ directory): upload model, baseline and data, then restart the backend:"
echo "  python model_store.py push-model --prefix ${SHIP} --baseline ml/candidate_baseline"
echo "  python model_store.py push-data"
echo "Local use: cp ${SHIP}_model.pkl ml/model_v2.pkl && cp ${SHIP}_encoder.pkl ml/label_encoder_v2.pkl"
