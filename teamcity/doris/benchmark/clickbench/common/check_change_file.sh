#!/bin/bash

#res=(`git diff --name-only HEAD~ HEAD`)
#file_nums=${#res[@]}
owner=apache
repo=doris
GITHUB_TOKEN=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

usage() {
  echo "
Usage: $0 <options>
  Optional options:

    $0                                  check file changed api
    $0 --is_modify_only_invoved_be pr_id          if pr changed code only invoved be, doc, fs_brocker, return 0; else return 2
    $0 --is_modify_only_invoved_fe pr_id          if pr changed code only invoved fe doc, fs_brocker, return 0; else return 2
    $0 --is_modify_only_invoved_doc pr_id         if pr changed code only invoved doc, fs_brocker, return 0; else return 2
  "
  exit 1
}


function get_all_change_files(){
    pr_id=$1
    files=()

    page=1
    per_page=100
    while [ 1 -eq 1 ]
    do
        #echo "curl --header 'authorization: Bearer ${GITHUB_TOKEN}' https://api.github.com/repos/${owner}/${repo}/pulls/${pr_id}/files?per_page=$per_page\&page=$page"
        res=($(curl --header "authorization: Bearer ${GITHUB_TOKEN}" https://api.github.com/repos/${owner}/${repo}/pulls/${pr_id}/files?per_page=$per_page\&page=$page|jq -r '.[]|select(.status = "removed")| .filename'))
        res_len=${#res[@]}
        #echo "================"$res_len
        if [ ${res_len} -ne 0 ];then
            let page+=1
            #files="${res[@]}""$files"
            files=(${res[@]} ${files[*]})
        else
            break
        fi
        echo
    done
    echo "${files[@]}"
}


function check_removed_change_file(){
    pr_id=$1
    module=$2
    owner='apache'
    repo='doris'
    #res=($(curl https://api.github.com/repos/${owner}/${repo}/pulls/${pr_id}/files|jq -r '.[]|select(.status = "removed")| .filename'))
    res=($(get_all_change_files $pr_id))
    file_nums=${#res[@]}
    is_code_change_flag=false
    module_file=0
    for file in ${res[@]}
    do
	#echo "$file"
	file_dir=$(echo $file|cut -d '/' -f 1)
        file_type=$(echo $file|rev|cut -d / -f 1|cut -d '.' -f 1|rev)
	#if [[  "check$file_dir" == "check${module}" &&  ($file_type == "cpp" || $file_type == "c" || $file_type == "java" || $file_type == "py" || $file_type == "h" || $file_type == 'js') ]];then
	if [[ $file_type == "cpp" || $file_type == "c" || $file_type == "java" || $file_type == "py" || $file_type == "h" || $file_type == 'js' ]];then
	    echo "code has changed, ${file} is deleted"
	    is_code_change_flag=true
	fi
	if [[ "check$file_dir" == "check${module}" ]];then
            let module_file+=1
	fi
    done

    if [[ "${is_code_change_flag}" == "false" ]];then
	echo "NO CODE FILE DELETED, PASSED!"
        return 0
    else
	echo "CODE FILE BE DELETED"
        return 2
    fi

}

function check_all_change_files_is_under_doc() {

    pr_id=$1
    owner='apache'
    repo='doris'
    #res=($(curl https://api.github.com/repos/${owner}/${repo}/pulls/${pr_id}/files|jq -r '.[]|select(.status != "removed")| .filename'))
    res=($(get_all_change_files $pr_id))
    #echo "======this pr change file is:======="
    #echo "${res[@]}"
    #echo

    file_nums=${#res[@]}

    doc_num=0
    doc_sql_manual_num=0
    for file in ${res[@]}
    do
        #check change file is on docs/fs_brokers or not
	echo "$file"
        file_dir=$(echo $file|cut -d '/' -f 1)
	if [[ $file_dir == "docs" ]];then
            let doc_num+=1
	fi
        if [[ "$file" =~ "docs/zh-CN/docs/sql-manual/" || "$file" =~ "docs/en/docs/sql-manual/" ]];then
            let doc_sql_manual_num+=1
            continue
        fi


        #check change file is md/txt/doc file
        #file_type=$(echo $file|cut -d '.' -f 2)
        #if [[ $file_type == "md" || $file_type == "txt" || $file_type == "doc" ]];then
        #    let doc_num+=1
        #fi
    done

    echo "======================"
    echo "doc_num: $doc_num"
    echo "file_nums: $file_nums"
    echo "doc_sql_manual_num: $doc_sql_manual_num"
    if [[ $doc_num -eq $file_nums && $doc_sql_manual_num -eq 0 ]];then
        echo "JUST MODIFY DOCUMENT, NO COED CHSNGED, PASSED!"
        return 0
    else
        echo "FILE UNDER docs/*/docs/sql-manual IS CHANGED, TRIGGER PIPLINE!"
        return 2
    fi
}

function check_all_change_files_is_under_be() {

    pr_id=$1
    owner='apache'
    repo='doris'
    #res=($(curl https://api.github.com/repos/${owner}/${repo}/pulls/${pr_id}/files|jq -r '.[]|select(.status != "removed")| .filename'))
    res=($(get_all_change_files $pr_id))
    file_nums=${#res[@]}
    #echo "======this pr change file is:======= \n"
    #echo "${res[@]}"
    #echo

    doc_num=0
    echo "START CHECK CODE IS ONLY RELATED BE OR NOT"
    for file in ${res[@]}
    do
        echo "$file"
        #check change file is on be or not
        file_dir=$(echo $file|cut -d '/' -f 1)
        if [[ $file_dir == "be" || $file_dir == "docs" || $file_dir == "fs_brokers" ]];then
            let doc_num+=1
            continue
	fi
    done
    if [[ $doc_num -eq $file_nums ]];then
        echo "JUST MODIFY BE CODE, NO NEED RUN FE UT, PASSED!"
        return 0
    else
        echo "NOT ONLY BE CODE CHANGED, TRIGGER PIPLINE!!"
        return 2
    fi
}

function check_all_change_files_is_under_fe() {

    pr_id=$1
    owner='apache'
    repo='doris'
    #res=($(curl https://api.github.com/repos/${owner}/${repo}/pulls/${pr_id}/files|jq -r '.[]|select(.status != "removed")| .filename'))
    res=($(get_all_change_files $pr_id))
    file_nums=${#res[@]}
    #echo "======this pr change file is:======= \n"
    #echo "${res[@]}"
    #echo

    doc_num=0
    echo "START CHECK CODE IS ONLY RELATED FE OR NOT"
    for file in ${res[@]}
    do
        echo "$file"
        #check change file is on be or not
        file_dir=$(echo $file|cut -d '/' -f 1)
        if [[ $file_dir == "fe" || $file_dir == "docs" || $file_dir == "fs_brokers" ]];then
            let doc_num+=1
            continue
        fi
    done
    if [[ $doc_num -eq $file_nums ]];then
        echo "JUST MODIFY FE CODE, NO NEED RUN BE UT, PASSED!"
        return 0
    else
        echo "NOT ONLY FE CODE CHANGED, TRIGGER PIPLINE!"
        return 2
    fi
}

main() {

if [ $# > 0 ]; then
    case "$1" in
        --is_modify_only_invoved_be)
	    check_removed_change_file $2 "be"
	    check_1=$?
	    check_all_change_files_is_under_be $2
	    check_2=$?
	    echo ${check_1}
	    echo ${check_2}
	    res=`expr $check_1 \* $check_2`
	    exit $res
	    shift ;;
        --is_modify_only_invoved_fe)
	    check_removed_change_file $2 "fe"
	    check_1=$?
	    check_all_change_files_is_under_fe $2
	    check_2=$?
            res=`expr $check_1 \* $check_2`
	    echo ${check_1}
	    echo ${check_2}
	    exit $res
	    shift ;;
	--is_modify_only_invoved_doc)
	    check_removed_change_file $2 "docs"
	    check_1=$?
	    check_all_change_files_is_under_doc $2
	    check_2=$?
            res=`expr $check_1 + $check_2`
	    echo ${check_1}
            echo ${check_2}
	    exit $res
	    shift ;;
        *) echo "ERROR"; usage; exit 1 ;;
    esac

fi
}

main $@
