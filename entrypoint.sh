#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
        sleep 20
        exec yarn start | bunyan
        ;;
    test)
        echo "Running Test"
        exec yarn test
        ;;
    start)
        echo "Running Start"
        exec yarn start
        ;;
    *)
        exec "$@"
esac
