set colsep ','
set echo off
set feedback off
set pagesize 0
set trimspool on
set headsep off
set linesize 300
set sqlprompt ''

select HLTKEY,HLTPATHNAME,L1SEED from CMS_LUMI_PROD.TRGHLTMAP where L1SEED like '%NOT%' or L1SEED like '% AND %NOT%' or L1SEED like '% AND % OR %' or L1SEED like '% OR % AND %';

spool trghltmap_strange.dat
/

