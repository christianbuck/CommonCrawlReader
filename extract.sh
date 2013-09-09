#!/bin/bash
export CLASSPATH="/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/commons-logging-1.1.1.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/guava-11.0.2.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/commons-configuration-1.6.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/commons-lang-2.5.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/hadoop-auth-2.1.0-beta.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/slf4j-log4j12-1.6.1.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/slf4j-api-1.6.1.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/log4j-1.2.17.jar:/home/buck/build/hadoop-2.1.0-beta/share/hadoop/common/lib/avro-1.5.3.jar:/home/buck/Dropbox/src/ccreader/dist/ccreader.jar" 
TMP_DATA=/home/buck/net/buri/cc/1346823845675/tmp/`basename $1`
echo $TMP_DATA
mkdir -p $TMP_DATA
java ccreader.Ccreader $1 | nice langid/sort.py ${TMP_DATA}/data
pushd $TMP_DATA
for f in *.gz
do
  echo 'sentence splitting ' $f
  L=`echo ${f/data_/} | cut -d '.' -f 1`
  zcat $f | nice ~/build/mosesdecoder/scripts/ems/support/split-sentences.perl -l $L | \
            nice xz > ${f/.gz/.xz}
  rm -vf $f
done
popd
