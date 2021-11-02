#!/bin/sh
if [ ! -d /home/ubuntu/outdir ]
then
    mkdir /home/ubuntu/outdir
fi
cp -r /home/ubuntu/scratch/cloudspeedtest/outdir/* /home/ubuntu/outdir/
#restore possibly wrong active sibling
mv /home/ubuntu/outdir/datafiles/*.sibling.active /home/ubuntu/outdir/datafiles/*.sibling.txt
echo "google" >/home/ubuntu/vmtype
mv /home/ubuntu/outdir/datafiles/google.sibling.txt /home/ubuntu/outdir/datafiles/google.sibling.active
rm -rf /home/ubuntu/scratch 
