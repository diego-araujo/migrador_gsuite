#!/bin/bash
URL="$1:5enh@_@ce55* https://correio.embrapa.br/home/$1/$2?fmt=$3"
RESPONSE=`curl --silent -k -u $URL `
echo "$RESPONSE"
