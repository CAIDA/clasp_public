#!/usr/local/bin/bash
HOSTFILE="/scratch/cloudspeedtest/cloudhosts"
for arg in "$@"
do
    case $arg in
        -i=*|--include=*)
            NODES=$(echo ${arg#*=}|tr "," "\n")
            shift
            ;;
        -f=*|--hostfile=*)
            HOSTFILE="${arg#*=}"
            shift
            ;;
        -p=*|--provider=*)
            PROVIDER="${arg#*=}"
            shift
            ;;
        -h|--help)
            echo "Generate series of -o <hostname> using host alias"
            echo "usage: -i=node1,node2 -f=path/to/hostfile -p=provider"
            echo " -i    comma seperated string of host aliases, also accept simple regex"
            echo " -f    file contained host aliases"
            echo " -p    only list hosts from this provider"
            echo " -h    print this usage"
            exit 0
            shift
            ;;
    esac
done

if [[ $NODES == "" ]]; then
	NODES=`awk '{print $1}' $HOSTFILE`
fi

if [[ $NODES != "" ]]; then
    AMAZONHOST=""
    AZUREHOST=""
    GOOGLEHOST=""
    GOOGLESTDHOST=""
    for node in $NODES
    do
        #can be more than one host matched the regex
        HOSTS="$HOSTS "`grep "^$node\s" $HOSTFILE | awk '{print $1}'`
    done
    UHOSTS=`echo $HOSTS|tr " " "\n"|sort|uniq`
    for host in $UHOSTS
    do
        hname=`grep "^$host\s" $HOSTFILE |awk '{print $2}'`
#        HOSTSTR="$HOSTSTR -o ${hname}"
        provider=`grep "^$host\s" $HOSTFILE $STDHOSTFILE | awk '{print $3}'`
        case $provider in
            amazon)
                AMAZONHOST="$AMAZONHOST -o ${hname}"
                ;;
            azure)
                AZUREHOST="$AZUREHOST -o ${hname}"
                ;;
            google)
                GOOGLEHOST="$GOOGLEHOST -o ${hname}"
                ;;
            googlestd)
                GOOGLESTDHOST="$GOOGLESTDHOST -o ${hname}"
        esac
    done
fi
case $PROVIDER in
    amazon)
        if [[ $AMAZONHOST != "" ]]; then
            echo $AMAZONHOST
        fi
        ;;
    azure)
        if [[ $AZUREHOST != "" ]]; then
            echo $AZUREHOST
        fi
        ;;
    google)
        if [[ $GOOGLEHOST != "" ]]; then
            echo $GOOGLEHOST
        fi
        ;;
    googlestd)
        if [[ $GOOGLESTDHOST != "" ]]; then
            echo $GOOGLESTDHOST
        fi
        ;;        
    *)
        echo $AMAZONHOST $AZUREHOST $GOOGLEHOST $GOOGLESTDHOST
        ;;
esac


