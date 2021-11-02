#!/bin/bash
MONTH=$(date -d "$D" '+%m')
YEAR=$(date -d "$D" '+%Y')

FIRSTDATE="$YEAR""$MONTH""01"
echo $FIRSTDATE

ASRANKRIB="/data/external/as-rank-ribs/$FIRSTDATE"

ANADIR="/scratch/cloudspeedtest/analysis"
OUTDIR="/scratch/cloudspeedtest/outdir/datafiles"

if [ -d "$ASRANKRIB" ]
then
    cp $ASRANKRIB/$FIRSTDATE.prefix2as.bz2 $ANADIR/prefix2as
    cp $ASRANKRIB/$FIRSTDATE.as-rel.txt.bz2 $ANADIR/as-rel

    bunzip2 $ANADIR/prefix2as/$FIRSTDATE.prefix2as.bz2
    bunzip2 $ANADIR/as-rel/$FIRSTDATE.as-rel.txt.bz2
    
    rm $OUTDIR/*.prefix2as
    cp $ANADIR/prefix2as/$FIRSTDATE.prefix2as $OUTDIR
else
    echo "AS RANK RIB not exist"
fi


