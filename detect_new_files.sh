#!/bin/bash

#Create array of newly added files
array=()
while IFS=  read -r -d $'\0'; do
    array+=("$REPLY")
done < <(find ${LENDING_CLUB_DIR}/data/*csv  -newer ${LENDING_CLUB_DIR}/data/stamp  -print0)

#Update stamp
touch ${LENDING_CLUB_DIR}/data/stamp

#Process each new file
for f in ${array[@]};
do
   bash ${LENDING_CLUB_DIR}/run.sh $f
done
