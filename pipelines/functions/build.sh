#!/bin/sh

for filename in test.py
do
	NAME=$(echo $filename | cut -f 1 -d '.')
	TMP=/tmp/$NAME
	mkdir $TMP
	cp requirements.txt $TMP/
	cp $filename $TMP/__main__.py
	
	docker run --rm -v "$TMP:/tmp" openwhisk/python2action bash  -c "cd tmp && apk update && apk add git && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r requirements.txt"

	cd $TMP
	zip -r functions.zip virtualenv __main__.py

	bx wsk action delete $NAME

	bx wsk action create $NAME --kind python:2 --web true --main executePipeline $TMP/functions.zip
	bx wsk api create /marketinsights /$NAME get $NAME --response-type json

	#sudo rm -fr $TMP

done