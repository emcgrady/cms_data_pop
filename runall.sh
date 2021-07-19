#!/usr/bin/env bash
echo $HOSTNAME|grep -q 'ithdp-client' || return
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix 3.2 spark3

dates="2021/*/*"
outprefix="root://eosuser/eos/user/c/cmoore/hadoop-transfer/" 

#hdfs dfs -rm -R hdfs://analytix/user/$USER/working_set_cmssw
#spark-submit generate_working_set.py --source cmssw --out hdfs://analytix/user/$USER/working_set_cmssw --dates $dates
#hdfs dfs -cp hdfs://analytix/user/$USER/working_set_cmssw $outprefix
##
#hdfs dfs -rm -R hdfs://analytix/user/$USER/working_set_fwjr
#spark-submit generate_working_set.py --source fwjr --out hdfs://analytix/user/$USER/working_set_fwjr --dates $dates
#hdfs dfs -cp hdfs://analytix/user/$USER/working_set_fwjr $outprefix
##
#hdfs dfs -rm -R hdfs://analytix/user/$USER/working_set_classads
#spark-submit generate_working_set.py --source classads --out hdfs://analytix/user/$USER/working_set_classads --dates $dates
#hdfs dfs -cp hdfs://analytix/user/$USER/working_set_classads $outprefix
##
#hdfs dfs -rm -R hdfs://analytix/user/$USER/working_set_xrootd
#spark-submit generate_working_set.py --source xrootd --out hdfs://analytix/user/$USER/working_set_xrootd --dates $dates
#hdfs dfs -cp hdfs://analytix/user/$USER/working_set_xrootd $outprefix
##
hdfs dfs -cp hdfs://analytix/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/BLOCKS/part-m-00000 ${outprefix}dbs_blocks.csv

hdfs dfs -cp hdfs://analytix/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATA_TIERS/part-m-00000 ${outprefix}dbs_datatiers.csv
