#!/bin/bash
cat $1 | mysql --login-path=$2
