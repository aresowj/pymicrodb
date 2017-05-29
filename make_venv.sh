#!/usr/bin/env bash

virtualenv venv
. ./venv/bin/activate
pip install -U -r requirements_test.txt

