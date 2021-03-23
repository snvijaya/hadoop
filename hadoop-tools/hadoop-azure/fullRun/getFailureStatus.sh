tmpfileName=errs/errCount
fileName=errs/TestResult
[ -f $fileName ] && rm $fileName
[ -f $tmpfileName ] && rm $tmpfileName
grep " » " *Test* > $fileName
for f in *Test*; do
   grep '[ERROR]' $f | grep "Tests run:" | grep -v "0, Skipped:" | tail -1 | grep -oE "Errors: [0-9]+" | grep -oE "[0-9]+" >> $tmpfileName
done

SUM=0
for num in $(cat $tmpfileName)
    do
        ((SUM+=num))
done
echo "---------------------------------" >> $fileName
echo "Total failed testcases = $SUM" >> $fileName
echo "---------------------------------" >> $fileName
rm $tmpfileName
cat $fileName
