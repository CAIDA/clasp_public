#!/bin/sh
if [ ! -d /home/caida/outdir ]
then
    mkdir /home/caida/outdir
fi
cp -r /home/caida/scratch/cloudspeedtest/outdir/* /home/caida/outdir/
#restore possibly wrong active sibling
mv /home/caida/outdir/datafiles/*.sibling.active /home/caida/outdir/datafiles/*.sibling.txt
echo "googlestd" >/home/caida/vmtype
mv /home/caida/outdir/datafiles/google.sibling.txt /home/caida/outdir/datafiles/google.sibling.active
rm -rf /home/caida/scratch 
