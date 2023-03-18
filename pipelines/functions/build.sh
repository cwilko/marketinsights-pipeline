#!/bin/sh

# Ensure the user is logged in to the correct IBM Cloud org and space (e.g. eu-gb/MIOL_prod)
# ibmcloud login
# ibmcloud target -g default

for filename in marketdirection.py
do
	NAME=$(echo $filename | cut -f 1 -d '.')

	echo "Cleaning up previous function"
	# Use the MIOL_namespace which should be an IAM-namespace
	ibmcloud fn namespace target MIOL_namespace
	ibmcloud fn action delete $NAME

	echo "Publishing function"
	# Not a web action (unauthenticated)
	ibmcloud fn action create $NAME --docker cwilko/marketinsights-function-deps:0.0.1 --main executePipeline $filename

done