#!/bin/bash
HOME=.
nohup sh $HOME/bin/polygon_gulf.sh 2>&1 &
nohup sh $HOME/bin/polygon_taxi.sh 2>&1 &
wait
