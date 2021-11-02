#!/usr/bin/bash
LNAME=`logname`
if [[ $LNAME == "" ]]; then
    LNAME="ubuntu"
fi

DATAFILEDIR=${1%/}
if [ -z "$DATAFILEDIR" ]
then
    DATAFILEDIR="outdir/datafiles"
fi
OUTPUTDIR=${2%/}

if [ -z "$OUTPUTDIR" ]
then
    OUTPUTDIR="results/trace"
fi
STARTTS=`date +%s`

FIRSTHOP=5
HOSTTYPE=`cat /home/$LNAME/vmtype`
if [[ $HOSTTYPE == "amazon" ]]; then FIRSTHOP=5; fi
if [[ $HOSTTYPE == "azure" ]]; then FIRSTHOP=6; fi
if [[ $HOSTTYPE == "google" ]]; then FIRSTHOP=1; fi
if [[ $HOSTTYPE == "googlestd" ]]; then FIRSTHOP=1; fi

HNAME=`cat /home/$LNAME/hostalias`
FILEPREFIX=$HNAME.$STARTTS

SCCHECK=`ps aux|grep '^root.*scamper'|wc -l`
if [[ $SCCHECK == "0" ]]; then
    echo "starting scamper"
    sudo scamper -D -p1000 -P12345;
fi

[ ! -d "/home/$LNAME/tmp" ] && mkdir /home/$LNAME/tmp
TMPDIR=$(mktemp -d -t tr-XXXXXXXX --tmpdir=/home/$LNAME/tmp)

METAFILE=$TMPDIR/$FILEPREFIX.meta

for files in `ls $DATAFILEDIR/*.ip`; do
    IPFILELIST=${files##*/}
    MSUM=`md5sum $files`
    echo $MSUM >> $METAFILE
    OUTPUTIPLIST="$TMPDIR/$FILEPREFIX.$IPFILELIST.trace.warts"
    TRCMD="trace -f $FIRSTHOP -P udp-paris -w 1"
    sudo sc_attach -c "$TRCMD" -p 12345 -i $files -o $OUTPUTIPLIST
done

[ ! -d $OUTPUTDIR ] && mkdir -p $OUTPUTDIR
cd $TMPDIR; sudo tar cjf /home/$LNAME/$OUTPUTDIR/$FILEPREFIX.trace.tar.bz2 $FILEPREFIX.* --remove-files
cd /home/$LNAME;
sudo rm -rf $TMPDIR
echo "TRACEROUTE ends"
