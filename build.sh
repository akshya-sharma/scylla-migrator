#!/bin/bash

set -e
set -x

export TERM=xterm-color

sbt -mem 2048 migrator/assembly
