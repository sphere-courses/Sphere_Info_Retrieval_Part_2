#!/usr/bin/env bash

/home/nahodnov17/Starspace/starspace train \
  -trainFile ./../data/docs.tsv/docs_part_large \
  -model ./model \
  -trainMode 2 \
  -initRandSd 0.01 \
  -adagrad true \
  -ngrams 1 \
  -lr 0.05 \
  -epoch 5 \
  -thread 20 \
  -dim 100 \
  -negSearchLimit 5 \
  -maxNegSamples 3 \
  -dropoutRHS 0.8 \
  -fileFormat labelDoc \
  -similarity "cosine" \
  -minCount 5 \
  -normalizeText true \
  -verbose true