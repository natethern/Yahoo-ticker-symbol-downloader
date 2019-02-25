echo Starting Backup
while true
do
echo Checking Files...
FILE_SIZE=`stat -f%z ./generic.pickle`
BACKUP_SIZE=`stat -f%z ./backup/generic.pickle`
if (( $FILE_SIZE > $BACKUP_SIZE ))
then
cp ./generic.pickle ./backup/generic.pickle
echo saved "$(date)"
else
echo Files not updated.
fi
sleep 300
done