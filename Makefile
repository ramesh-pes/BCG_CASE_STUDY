build:
	rm -rf ./dist && mkdir ./dist
	cp ./src/main.py ./dist
	cp ./src/config.ini ./dist
	cd ./src && zip -x main.py -x config.ini -r ../dist/utils.zip .

