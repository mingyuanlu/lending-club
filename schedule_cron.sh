#!/bin/sh

#Edit the following line to point to your local detect_new_files.sh
echo "0 0 * * * /bin/bash /Users/ming-yuanlu/Documents/Insight/coding_challenges/lending_club/detect_new_files.sh" >> mycron

#Install new cron file
crontab mycron
rm mycron
