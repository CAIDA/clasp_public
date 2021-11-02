#!/usr/local/bin/bash
KEYLOC="/scratch/webspeedtest/cloud/speedtest_id_rsa"
NODES=""
HOSTSTR=""
HOSTFILE="/scratch/cloudspeedtest/cloudhosts"
CMD=""
RUNSUDO=0
#HNAME=`grep $HOSTFILE /scratch/cloudspeedtest/cloudhosts|awk '{print $2}'`
for arg in "$@"
do
    case $arg in
        -i=*|--include=*)
            NODES=$(echo ${arg#*=})
            shift
            ;;
        -C=*|--cmd=*)
            CMD="${arg#*=}"
            shift
            ;;
        -p=*|--provider=*)
            PROVIDER="${arg#*=}"
            shift
            ;;
        -k=*|--key=*)
            KEYLOC="${arg#*=}"
            shift
            ;;
        -u)
            RUNSUDO=1
            shift
            ;;
        -h|--help)
            echo "usage: ./batchsetup.bash [-i=host1,host2,...] [-k=privatekey]"
            exit 0
            ;;

    esac
done
if [[ $NODES == "" ]]; then
    awk '{print $2}' $HOSTFILE | orgalorg -u ubuntu -k $KEYLOC -C $CMD;
else
    HOSTS=`bash searchhost.bash -i="$NODES" -p="$PROVIDER"`
    if [[ $RUNSUDO == 0 ]]; then
        echo "Run as normal user"
        echo $HOSTS
        orgalorg $HOSTS -k $KEYLOC -u ubuntu  -C $CMD;
    else
        echo "Run as root $AMAZONHOST $CMD"
        orgalorg $HOSTS -k $KEYLOC -u ubuntu -x -C $CMD;
    fi
fi
