#### CS 165 milestone test script           ####
# this script takes generated tests,
# and kicks off running a test for each,
# via a hacky call of makefile target for a
# single test case to be run
#
#### Contact: Wilson Qin                    ####

UPTOMILE="${1:-5}"

MAX_AVAILABLE_MS=5
MAX_TEST=43
TEST_IDS=`seq -w 1 ${MAX_TEST}`

if [ "$UPTOMILE" -eq "1" ] ;
then
    MAX_TEST=9
elif [ "$UPTOMILE" -eq "2" ] ;
then
    MAX_TEST=19
elif [ "$UPTOMILE" -eq "3" ] ;
then
    MAX_TEST=29
elif [ "$UPTOMILE" -eq "4" ] ;
then
    MAX_TEST=39
elif [ "$UPTOMILE" -eq "5" ] ;
then
    MAX_TEST=43
fi

for TEST_ID in $TEST_IDS
do
    if [ "$TEST_ID" -le "$MAX_TEST" ]
    then
        make run_test test_id=$TEST_ID
    fi
done
