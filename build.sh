#!/bin/bash

./configure \
    --with-flex=/usr/local/Cellar/flex/2.5.37/bin/flex \
    --with-bison=/usr/local/Cellar/bison/3.0.2/bin/bison && make build
