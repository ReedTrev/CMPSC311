#!/bin/bash
#Alison Pastore
#Trevor Reed

#Store start time
STARTTIME=$(date +%s%N)

if [ -f "$1" ] #Check if file exists
then
        #Remove special characters, put each word in it's own line, sort words by line and write to the output file
        less $1 | tr -cd 'a-zA-Z0-9 \n' |
        tr "\n" " " |
        tr " " "\n" |
        grep '[^[:blank:]]' |
        sort -f |
        sed 's/$/, /g' |
        uniq -c |
        sed -e 's/^ *\([0-9]\+\) \(.\+\)/\2\1/' > $2 #Write to output file
else
        echo "Input file does not exist."
        exit 1
fi

#Store end time
ENDTIME=$(date +%s%N)

#Calculate execution time and write to time file
printf '%d nanoseconds have passed \n' $((ENDTIME - STARTTIME))  >> $3
