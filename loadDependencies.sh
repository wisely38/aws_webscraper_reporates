#!/bin/sh

pip freeze > requirements.txt
pip install -r requirements.txt -t ./package
