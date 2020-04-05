#!/usr/bin/bash

# path to this script's parent directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR
git clone git@github.com:CSSEGISandData/COVID-19.git

conda env create -f conda.yaml
