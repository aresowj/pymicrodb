#!/usr/bin/env bash

old_path=$(pwd)
old_pythonpath=$PYTHONPATH
. ./venv/bin/activate
export PYTHONPATH=$PYTHONPATH:$PWD

nosetests --cover-package=pymicrodb --tests=tests/ --with-coverage --cover-erase -v

cd "$old_path"
export PYTHONPATH=$old_pythonpath
