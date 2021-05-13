#!/bin/sh

# Ensure the user is logged in to the correct IBM Cloud org and space (e.g. eu-gb/MIOL_prod)
for filename in marketdirection.py
do
	NAME=$(echo $filename | cut -f 1 -d '.')
	TMP=/tmp/$NAME
	mkdir $TMP
	cp requirements.txt $TMP/
	cp $filename $TMP/__main__.py

	docker run --rm -v "$TMP:/tmp" ibmfunctions/action-python-v3 bash  -c "cd tmp && apt-get update && apt-get install -y git && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r requirements.txt"

	cd $TMP
	zip -r functions.zip virtualenv/bin/activate_this.py virtualenv/lib/python3.6/site-packages/quantutils __main__.py

	# Use the MIOL_namespace which should be an IAM-namespace
	ibmcloud fn namespace target MIOL_namespace
	ibmcloud cloud-functions action delete $NAME
	# Not a web action (unauthenticated)
	ibmcloud cloud-functions action create $NAME --kind python-jessie:3 --main executePipeline $TMP/functions.zip

	sudo rm -fr $TMP

done