#!/bin/bash
/usr/lib/postgresql/17/bin/psql --port=5437 --dbname="robohub" --command="SET search_path TO robo_slicer;" --command="SELECT robo_slicer.slicer_stream();"
