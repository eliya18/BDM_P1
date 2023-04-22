#! /bin/bash

echo "Getting third dataset from opendata..."
python opendatacollector.py
echo "Finished getting data from opendata..."

echo "Uploading all local files to temporal..."
python uploadToTemporalLanding.py
echo "Finished uploading files to temporal."

echo "Moving temporal files to persistent..."
python uploadToTemporalLanding.py
echo "Finished moving files to persistent."