HOMEDIR=/home/hdp-bigdatadev@office.corp.indosat.com/devops/awk-test

## Parse script arguments
while [ ! -z "$1" ]; do
  case "$1" in
     -hour)
         shift
         HRID=${1}
         ;;
     -date)
         shift
         DTID=${1}
         ;;
     -dir)
         shift
         DIR=${1}
         ;;
     *)
  esac
shift
done

if [ -z "${DIR}" ]
then
  echo "No directory argument being passed"
  echo "Aborting script execution"
  exit 1
fi

if [ -z "${DTID}" ] && [ -z "${HRID}" ]; then
  arr=($(sh ${HOMEDIR}/get_partition.sh -dir ${DIR} -res 7))
  if [ ${#arr[@]} -gt 0 ]; then
    printf -v joined "'%s'," "${arr[@]}"
    echo "${joined%,}"
  else exit 1
  fi
  exit 0
fi

if [ -z "${DTID}" ]
then
  echo "No date argument being passed"
  echo "Aborting script execution"
  exit 1
fi

if [ -z "${HRID}" ]
then
  echo "No hour argument being passed"
  echo "Aborting script execution"
  exit 1
fi

arr=($(sh ${HOMEDIR}/get_partition.sh -dir ${DIR} -date ${DTID} -hour ${HRID} -res 7))

if [ ${#arr[@]} -gt 0 ]; then
  printf -v joined "'%s'," "${arr[@]}"
  echo "${joined%,}"
else exit 1
fi
