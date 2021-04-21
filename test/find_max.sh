#!/bin/bash

cat $1 | grep "total run time" | grep "pass: 65000" | tail -10 | sort -n -k5 | tail -1
cat $1 | grep "total run time" | grep "pass: 50000" | tail -10 | sort -n -k5 | tail -1
