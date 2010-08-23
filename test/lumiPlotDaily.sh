#!/bin/sh
currendir=`pwd`
workdir="/build1/zx/cron/CMSSW_3_7_0_pre3"
authdir="/afs/cern.ch/user/x/xiezhen"
overviewdir="/afs/cern.ch/cms/lumi/www/plots/overview"
operationdir="/afs/cern.ch/cms/lumi/www/plots/operation"
logpath="/afs/cern.ch/cms/lumi"
logname="lumiPlotDaily.log"
logfilename="$logpath/$logname"
dbConnectionString="oracle://cms_orcoff_prod/cms_lumi_prod"
physicsselectionFile="/afs/cern.ch/user/x/xiezhen/StreamExpress/goodrunlist_json.txt"
beamenergy="3.5e03"
beamstatus="STABLE BEAMS"
beamfluctuation="0.2e03"

source /afs/cern.ch/cms/cmsset_default.sh;
cd $workdir
eval `scramv1 runtime -sh`
touch $logfilename
date >> $logfilename
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $overviewdir -beamenergy $beamenergy -beamfluctuation $beamfluctuation $beamstatus $beamstatus --withTextOutput totalvstime >> $logfilename 
sleep 1
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $overviewdir -beamenergy $beamenergy -beamfluctuation $beamfluctuation $beamstatus $beamstatus --withTextOutput perday >> $logfilename 
sleep 1;
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $operationdir --withTextOutput instpeakvstime >> $logfilename 
sleep 1
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $operationdir -beamenergy $beamenergy -beamfluctuation $beamfluctuation $beamstatus $beamstatus --withTextOutput totalvsfill >> $logfilename 
sleep 1
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $operationdir -beamenergy $beamenergy -beamfluctuation $beamfluctuation $beamstatus $beamstatus --withTextOutput totalvsrun >> $logfilename 
sleep 1
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $operationdir -i $physicsselectionFile --withTextOutput physicsvstime >> $logfilename
sleep 1
lumiPlotFiller.py -c $dbConnectionString -P $authdir -o $operationdir -i $physicsselectionFile --withTextOutput physicsperday >> $logfilename
cd $currentdir
