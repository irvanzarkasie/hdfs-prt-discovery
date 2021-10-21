HOMEDIR=/home/hdp-bigdatadev@office.corp.indosat.com/devops/awk-test

## Parse script arguments
while [ ! -z "$1" ]; do
  case "$1" in
     -hour)
         shift
         HRID=${1}
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

if [ -z "${HRID}" ]
then
  echo "No hour argument being passed"
  echo "Aborting script execution"
  exit 1
fi

arr=($(sh ${HOMEDIR}/get_partition_by_mod_hour.sh -dir ${DIR} -hour ${HRID} -res 7))

if [ ${#arr[@]} -gt 0 ]; then
  printf -v joined "'%s'," "${arr[@]}"
  echo "${joined%,}"
else exit 1
fi
