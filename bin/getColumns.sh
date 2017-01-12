#!/bin/bash
if ! read -t 0; then
    echo "$*" | grep "^\ \ \ \ \ \ \ \ \ \ \ \ \"" | sort -u | sed 's/^[\ 	]*\"\(.*\)\".*/\1/'
fi

