#! /bin/bash

GRN="\e[42m"
RED="\e[31m"
RST="\e[0m"
GREEN_OK="[${GRN}ok${RST}]"
RED_FAIL="[${RED}fail${RST}]"

function verify_output
{
    MSG_TXT=$1
    ID=$2
    CUR_OUT_FILE=$3
    CUR_EXP_FILE=$4
    CUR_NO_COMM_FILE=$5


    #NEW Remove all commands up to calling the client, strip comments (-- to eol), remove trailing and leading whitespace and remove blank lines
    #ALSO last awk is making sure all decimals have 2 rounded digits.
     sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g' ${CUR_OUT_FILE} | awk '{sub("--.*$","",$0);print;}' | sed -e 's/^[[:space:]]*//g;s/[[:space:]]*$//g' | grep '[^[:blank:]]' | awk -F, '{  if ($1 ~ /\./) { printf("%0.2f",$1); } else { printf("%s",$1); } for(i=2;i<=NF;i++){ if ($i ~ /\./) { printf(",%0.2f",$i) } else { printf(",%s",$i); } } printf("\n"); }' > ${CUR_NO_COMM_FILE}

    # If necessary strip commands up to start of client
    if grep '\.\/client' ${CUR_NO_COMM_FILE}
      then
      sed '0,/\.\/client/d' ${CUR_NO_COMM_FILE} > tmpfile
      mv tmpfile ${CUR_NO_COMM_FILE}
      #echo "Stripping commands..."
    fi


    DIFF=`diff -B -w ${CUR_NO_COMM_FILE} ${CUR_EXP_FILE}`
	if [ -z "${DIFF}" ]
	then

			echo -e "Success! ${MSG_TXT} ${ID} passes! ${GREEN_OK}"
			echo "<${ENTRY_TXT}${ID}> <pass>"
	else
        #Now we check the sorted version of output
		TMP_FILE=`mktemp`
		TMP_FILE2=`mktemp`
		sort -n ${CUR_NO_COMM_FILE} > $TMP_FILE
		#Should have that already there
		sort -n ${CUR_EXP_FILE} > $TMP_FILE2
		DIFF_SORTED=`diff -B -w $TMP_FILE $TMP_FILE2`
		if [ -z "${DIFF_SORTED}" ]
		then
			echo -e "Success! ${MSG_TXT} ${ID} passes on the -- sorted -- output! ${GREEN_OK} \n\tPlease check this ${ENTRY_TXT}."
		else
            echo -e "Failure! ${MSG_TXT} ${ID} failed on both outputs. ${RED_FAIL}"
        fi
		rm $TMP_FILE $TMP_FILE2
    fi

}

verify_output 'Test:' $1 $2 $3 $4 $5
