#!/bin/bash

#Create array of newly added files
array=()
while IFS=  read -r -d $'\0'; do
    array+=("$REPLY")
done < <(find ./data/*csv  -newer ./data/stamp  -print0)

#Update stamp
touch ./data/stamp

#Process each new file
for f in ${array[@]};
do
   bash ./run.sh $f
done
