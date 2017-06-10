#!/bin/bash

PYSPARK_PYTHON=python3.5 PYSPARK_DRIVER_PYTHON=python3.5 spark-submit --master local[4] watset.py "$@"
