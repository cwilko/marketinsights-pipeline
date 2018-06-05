#!/bin/sh

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

	bx wsk action delete $NAME

	#bx wsk action create $NAME --kind python-jessie:3 --main executePipeline $TMP/functions.zip

	# Web Action (in case needing to expose via APIC)
	bx wsk action create $NAME --kind python-jessie:3 --web true --main executePipeline $TMP/functions.zip
	#bx wsk action create $NAME --kind python-jessie:3 --web true -a require-whisk-auth ***REMOVED*** --main executePipeline $TMP/functions.zip
	#bx wsk api create /marketinsights /$NAME get $NAME --response-type json

	sudo rm -fr $TMP

done