#!/usr/bin/bash
YRMONTH="202004"
DATADIR="/data/external/whois-database-dumps/"
ANALYSISDIR="/scratch/cloudspeedtest/analysis/"
if [ "$1" != "" ]; then
    YRMONTH="$1"
fi

mkdir delegationtmp;
cp "$DATADIR$YRMONTH/$YRMONTH""01.delegated-"* ./delegationtmp;
cd ./delegationtmp;
gunzip *.gz;
awk -F "|" '{if (/^#/) {next} if (($3=="ipv4") && (($7 == "assigned") || ($7=="allocated"))) print $0}' $YRMONTH"01.delegated-"*-extended.txt > $ANALYSISDIR/delegation/delegated-ipv4-$YRMONTH.txt

ASRELDIR="/data/external/as-rank-ribs/"
cp "$ASRELDIR$YRMONTH""01/$YRMONTH""01.as-rel.txt.bz2" . 
bunzip2 -c *.bz2 > $ANALYSISDIR/as-rel/$YRMONTH"01.as-rel.txt"

rm -rf delegationtmp
