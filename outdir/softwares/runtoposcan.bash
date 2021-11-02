#/bin/bash
LNAME=`logname`
DATAFILEDIR=${1%/}
OUTPUTDIR=${2%/}
PEERINGFILE=""
SIBLINGFILE=""
PREFIXASFILE=""
STARTTS=`date +%s`

FIRSTHOP=5
if [[ $LNAME == "" ]]; then
    LNAME="ubuntu"
fi
HOSTTYPE=`cat /home/$LNAME/vmtype`
if [[ $HOSTTYPE == "amazon" ]]; then FIRSTHOP=5; fi
if [[ $HOSTTYPE == "azure" ]]; then FIRSTHOP=6; fi
if [[ $HOSTTYPE == "google" ]]; then FIRSTHOP=1; fi
if [[ $HOSTTYPE == "googlestd" ]]; then FIRSTHOP=1; fi


#check if scamper daemon is running (as root)
SCCHECK=`ps aux|grep '^root.*scamper'|wc -l`
if [[ $SCCHECK == "0" ]]; then
    echo "starting scamper"
    sudo scamper -D -p1000 -P12345;
fi
#look for latest datafile in the datafile dir
for files in `ls -t $DATAFILEDIR/*`; do
    if [[ $files == *"peering" ]]; then PEERINGFILE=$files; fi
    if [[ $files == *"sibling.active" ]]; then SIBLINGFILE=$files; fi
    if [[ $files == *"prefix2as" ]]; then PREFIXASFILE=$files; fi
done

HNAME=`cat /home/$LNAME/hostalias`
FILEPREFIX=$HNAME.$STARTTS
BDROUTPUT="$OUTPUTDIR/$FILEPREFIX.bdrmap.warts"
METAOUTPUT="$OUTPUTDIR/$FILEPREFIX.bdrmap.meta"

#print the names of data file used into a meta file
echo "$STARTTS,$PEERINGFILE,$SIBLINGFILE,$PREFIXASFILE">$METAOUTPUT
echo "$METAOUTPUT"
for files in `ls $DATAFILEDIR/*.ip`; do
    IPFILELIST=${files##*/}
    OUTPUTIPLIST="$OUTPUTDIR/$FILEPREFIX.$IPFILELIST.trace.warts"
    TRCMD="trace -f $FIRSTHOP -P udp-paris"
    sudo sc_attach -c "$TRCMD" -p 12345 -i $files -o $OUTPUTIPLIST
    echo "Traceroute $IPFILELIST Completed"
done

sudo sc_bdrmap -O udp -a $PREFIXASFILE -v $SIBLINGFILE -x $PEERINGFILE -p 12345 -f $FIRSTHOP -o $BDROUTPUT > /dev/null
echo "BDRMAP Completed"

tar cfvj $OUTPUTDIR/$FILEPREFIX.tar.bz2 $OUTPUTDIR/$FILEPREFIX.* --remove-files
echo "Data Compression Completed"

#do not sync data if speedtest is running. Data will be uploaded together with speedtest data 
/usr/bin/flock -w 0 /tmp/sptestlock bash syncstoreage.bash
echo "Data Upload Completed"
