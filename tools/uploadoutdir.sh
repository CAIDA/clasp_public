#!/usr/local/bin/bash
HOSTSTR=""
UPLOADDIR="/scratch/cloudspeedtest/outdir/*"
KEYLOC="/scratch/webspeedtest/cloud/speedtest_id_rsa"
HOSTFILE="/scratch/cloudspeedtest/cloudhosts"
NODES=""
DATADIRUPDATE=0

#just print out the command, do not run
TESTRUN=0

for arg in "$@"
do
    case $arg in
        -i=*|--include=*)
            NODES=$(echo ${arg#*=})
            shift
            ;;
        -U=*|--upload=*)
            UPLOADDIR="${arg#*=}"
            shift
            ;;
        -k=*|--key=*)
            KEYLOC="${arg#*=}"
            shift
            ;;
        -f=*|--hostfile=*)
            HOSTFILE="${arg#*=}"
            shift
            ;;
        -d)
            DATADIRUPDATE=1
            shift
            ;;
        -t)
            TESTRUN=1
            shift
            ;;
        -h|--help)
            echo "usage: -i=node1,node2 -U=oject/to/upload -f=path/to/hostfile -k=your_private_key -d -t -h"
            echo " -i    comma seperated string of host aliases"
            echo " -d    upload the default outdir $UPLOADDIR"
            echo " -f    path to hostfile"
            echo " -t    just print out the command to debug without execution"
            echo " -h    print this usage"
            exit 0
            shift
            ;;
    esac
done

if [[ $NODES != "" ]]; then
    AMAZONHOST=`bash searchhost.bash -i=$NODES -p=amazon -f=$HOSTFILE`
    AZUREHOST=`bash searchhost.bash -i=$NODES -p=azure -f=$HOSTFILE`
    GOOGLEHOST=`bash searchhost.bash -i=$NODES -p=google -f=$HOSTFILE`
    GOOGLESTDHOST=`bash searchhost.bash -i=$NODES -p=googlestd -f=$HOSTFILE`

    if [[ $HOSTFILE == "/scratch/cloudspeedtest/cloudhosts" ]]; then
        if [[ $TESTRUN -eq 0 ]]; then
            orgalorg $AMAZONHOST $AZUREHOST $GOOGLEHOST -u ubuntu -k $KEYLOC -r /home/ubuntu --upload $UPLOADDIR
        else
            echo "orgalorg $AMAZONHOST $AZUREHOST $GOOGLEHOST -u ubuntu -k $KEYLOC -r /home/ubuntu --upload $UPLOADDIR"
        fi
    else 
        if [[ $TESTRUN -eq 0 ]]; then
            orgalorg $GOOGLESTDHOST -u caida -k $KEYLOC -r /home/caida --upload $UPLOADDIR
        else
            echo "orgalorg $GOOGLESTDHOST -u caida -k $KEYLOC -r /home/caida --upload $UPLOADDIR"
        fi
    fi


    if [[ $DATADIRUPDATE -eq 1 ]]; then
        if [[ $TESTRUN -eq 0 ]]; then
            if [[ $AMAZONHOST != "" ]]; then orgalorg $AMAZONHOST -u ubuntu -k $KEYLOC -r /home/ubuntu -i amazon.cmd -C sh; fi
            if [[ $AZUREHOST != "" ]]; then orgalorg $AZUREHOST -u ubuntu -k $KEYLOC -r /home/ubuntu -i azure.cmd -C sh; fi
            if [[ $GOOGLEHOST != "" && $HOSTFILE == "/scratch/cloudspeedtest/cloudhosts" ]]; then orgalorg $GOOGLEHOST -u ubuntu -k $KEYLOC -r /home/ubuntu -i google.cmd -C sh; fi
            if [[ $GOOGLESTDHOST != "" ]]; then orgalorg $GOOGLESTDHOST -u caida -k $KEYLOC -r /home/caida -i googlestd.cmd -C sh; fi
        else
            if [[ $AMAZONHOST != "" ]]; then echo "orgalorg $AMAZONHOST -u ubuntu -k $KEYLOC -r /home/ubuntu -i amazon.cmd -C sh"; fi
            if [[ $AZUREHOST != "" ]]; then echo "orgalorg $AZUREHOST -u ubuntu -k $KEYLOC -r /home/ubuntu -i azure.cmd -C sh"; fi
            if [[ $GOOGLEHOST != "" && $HOSTFILE == "/scratch/cloudspeedtest/cloudhosts" ]]; then echo "orgalorg $GOOGLEHOST -u ubuntu -k $KEYLOC -r /home/ubuntu -i google.cmd -C sh"; fi
            if [[ $GOOGLESTDHOST != "" ]]; then echo "orgalorg $GOOGLESTDHOST -u caida -k $KEYLOC -r /home/caida -i googlestd.cmd -C sh"; fi
        fi
    fi
else
#all hosts
    if [[ $HOSTFILE == "/scratch/cloudspeedtest/cloudhosts" ]]; then
        if [[ $TESTRUN -eq 0 ]]; then
            awk '{print $2}' $HOSTFILE | orgalorg -u ubuntu -k $KEYLOC -s -r /home/ubuntu --upload $UPLOADDIR
        else
            echo  "orgalorg -u ubuntu -k $KEYLOC -s -r /home/ubuntu --upload $UPLOADDIR" `awk '{print $2}' $HOSTFILE` ;
        fi
    else 
        if [[ $TESTRUN -eq 0 ]]; then
            awk '{print $2}' $HOSTFILE | orgalorg -u caida -k $KEYLOC -s -r /home/caida --upload $UPLOADDIR
        else
            echo  "orgalorg -u caida -k $KEYLOC -s -r /home/caida --upload $UPLOADDIR" `awk '{print $2}' $HOSTFILE` ;
        fi
    fi


    if [[ $DATADIRUPDATE -eq 1 ]]; then
        #move the files to correct path, and run cloud specific commands
        AMAZONHOST=`grep 'amazon' $HOSTFILE | awk '{print $2}'`
        if [[ $AMAZONHOST != "" ]]; then
            if [[ $TESTRUN -eq 0 ]]; then
                grep 'amazon' $HOSTFILE | awk '{print $2}'|orgalorg -u ubuntu -k $KEYLOC -s -i amazon.cmd -C sh;
            else
                echo  "orgalorg -u ubuntu -k $KEYLOC -s -i amazon.cmd -C sh" `grep 'amazon' $HOSTFILE | awk '{print $2}'`;
            fi
        fi
        AZUREHOST=`grep 'azure' $HOSTFILE | awk '{print $2}'`
        if [[ $AZUREHOST != "" ]]; then
            if [[ $TESTRUN -eq 0 ]]; then
                grep 'azure' $HOSTFILE | awk '{print $2}' | orgalorg -u ubuntu -k $KEYLOC -s -i azure.cmd -C sh;
            else
                echo "orgalorg -u ubuntu -k $KEYLOC -s -i azure.cmd -C sh" `grep 'azure' $HOSTFILE | awk '{print $2}'`;
            fi
        fi
        GOOGLEHOST=`grep 'google' $HOSTFILE | awk '{print $2}'`
        if [[ $GOOGLEHOST != "" && $HOSTFILE == "/scratch/cloudspeedtest/cloudhosts" ]]; then
            if [[ $TESTRUN -eq 0 ]]; then
                grep 'google' $HOSTFILE | awk '{print $2}' | orgalorg -u ubuntu -k $KEYLOC -s -i google.cmd -C sh;
            else
                echo "orgalorg -u ubuntu -k $KEYLOC -s -i google.cmd -C sh" `grep 'google' $HOSTFILE | awk '{print $2}'`
            fi
        fi
        GOOGLESTDHOST=`grep 'googlestd' $HOSTFILE | awk '{print $2}'`
        if [[ $GOOGLESTDHOST != "" ]]; then
            if [[ $TESTRUN -eq 0 ]]; then
                grep 'googlestd' $HOSTFILE | awk '{print $2}' | orgalorg -u caida -k $KEYLOC -s -i googlestd.cmd -C sh;
            else
                echo "orgalorg -u caida -k $KEYLOC -s -i googlestd.cmd -C sh" `grep 'googlestd' $HOSTFILE | awk '{print $2}'`
            fi
        fi
    fi
fi


