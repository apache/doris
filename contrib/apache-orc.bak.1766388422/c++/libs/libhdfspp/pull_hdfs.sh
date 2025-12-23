if [ -z "$1" ]; then
    echo "Usage: pull_hdfs [path_to_hdfs_git_root]"
    exit 1;
fi
if [ ! -d "$1" ]; then
    echo "$1 is not a directory"
fi
if [ ! -d "$1/hadoop-hdfs-project" ]; then
    echo "$1 is not the root of a hadoop git checkout"
fi

HADOOP_ROOT=$1
echo HADOOP_ROOT=$HADOOP_ROOT
OUT=$(readlink -m `dirname $0`)
echo OUT=$OUT
TS=$OUT/imported_timestamp

    cd $HADOOP_ROOT &&
    mvn -pl :hadoop-hdfs-native-client -Pnative compile -Dnative_make_args="copy_hadoop_files"
    (date > $TS; git rev-parse --abbrev-ref HEAD >> $TS; git log -n 1 >> $TS;  \
        echo "diffs: --------------" >> $TS; git diff HEAD >> $TS; \
        echo "       --------------" >> $TS)
    cd $OUT &&
    #Delete everything except for pull_hdfs.sh and imported_timestamp
    find . ! -name 'pull_hdfs.sh' ! -name 'imported_timestamp' ! -name '.' ! -name '..' -exec rm -rf {} + &&
    cp -R $HADOOP_ROOT/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfspp . &&
    cp -R $HADOOP_ROOT/hadoop-hdfs-project/hadoop-hdfs-native-client/target/main/native/libhdfspp/extern libhdfspp/ &&
    cd libhdfspp &&
	tar -czf ../libhdfspp.tar.gz * &&
	cd .. &&
	rm -rf libhdfspp &&
	date >> $TS