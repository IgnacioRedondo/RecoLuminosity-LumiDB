#!/bin/bash
inputdir="/data/cmsdata/json_DCSONLY"
outputdir="/data/cmsdata/json_DCSONLY_lumi"
logdir="/data/cmsdata/json_DCSONLY_lumi"
slist=$(seq 19 77)
#slist=(19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77)

for i in $slist;
do
  inputfile="$inputdir/json_DCSONLY_$i.txt"
  outputfile="${outputdir}/json_DCSONLY_lumi_${i}.csv"
  logfile="${outputdir}/json_DCSONLY_lumi_${i}.log"
  cmd="lumiCalc2.py lumibylsXing --xingMinLum 0.3 -b stable -i ${inputfile} -o ${outputfile}"
  echo $cmd
  $cmd >& ${logfile}
  sleep 2
done