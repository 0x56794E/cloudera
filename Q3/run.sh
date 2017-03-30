#!/bin/bash

rm *~
rm -rf test.out
rm *.log

pig -x local test.pig
