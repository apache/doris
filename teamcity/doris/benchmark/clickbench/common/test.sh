#!/bin/bash
owner=apache
repo=doris
GITHUB_TOKEN=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

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


out=($(get_all_change_files 11154))



echo "=================="
echo "${out[@]}"
