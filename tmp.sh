function run(str) {
    cd $str
    flist = `ls -l`
    for f in flist
    do
        if test -d $f then
            run $f
        else
            cat $f
        fi
    done
}