HOMEDIR=/home/hdp-bigdatadev@office.corp.indosat.com/devops/awk-test

kinit -kt ${HOMEDIR}/bigdatadev.keytab hdp-bigdatadev

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
     -res)
         shift
         RES=${1}
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

### SAMPLE INPUT:
### [START OF FILE]
###  Found 213 items
###  drwxrwx--x+  - hive hive          0 2019-05-19 21:19 /user/hive/warehouse/smy.db/ggsn_hourly_summary/.hive-staging_hive_2019-05-19_10-17-20_912_339861419533638038-11671
###  drwxrwx--x+  - hive hive          0 2021-08-08 18:05 /user/hive/warehouse/smy.db/ggsn_hourly_summary/recordopeningtime=20210207
### [END OF FILE]

hdfs dfs -ls -t -r ${DIR} |\
awk -v HRID="${HRID}" -v DTID="${DTID}" -v RES="${RES}" '

### Function definition for print single result content
function print_result_by_id(arr, id) { if (length(id) > 0) {print arr[id];} else {print_result(arr);} }\

### function definition for print whole result content
function print_result(arr) { print arr[1],arr[2],arr[3],arr[4],arr[5],arr[6],arr[7] }

{\

  ### $0 will yields:
  ### > Found 213 items
  ### > drwxrwx--x+  - hive hive          0 2019-05-19 21:19 /user/hive/warehouse/smy.db/ggsn_hourly_summary/.hive-staging_hive_2019-05-19_10-17-20_912_339861419533638038-11671
  ### > drwxrwx--x+  - hive hive          0 2021-08-08 18:05 /user/hive/warehouse/smy.db/ggsn_hourly_summary/recordopeningtime=20210207

  split($0,arr," "); \

  ### arr[6] will yields:
  ### > 
  ### > 2019-05-19
  ### > 2021-08-08

  ### arr[7] will yields:
  ### >
  ### > 21:19
  ### > 18:05

  ### arr[8] will yields:
  ### >
  ### > /user/hive/warehouse/smy.db/ggsn_hourly_summary/.hive-staging_hive_2019-05-19_10-17-20_912_339861419533638038-11671
  ### > /user/hive/warehouse/smy.db/ggsn_hourly_summary/recordopeningtime=20210207

 
  if (length(arr[6]) > 0) {\
    
    # split full path of a directory by '/' chracter
    # n will yields array length of the splitted string
    n=split(arr[8], prtarr, "/");\

    # split last modified time information
    split(arr[7], timearr, ":");\

    # split last modified date information
    split(arr[6], datearr, "-");\
  } else {}; \

  ### prtarr will consists of (represented as comma-delimited array):
  ### >
  ### > ,user,hive,warehouse,smy.db,ggsn_hourly_summary,.hive-staging_hive_2019-05-19_10-17-20_912_339861419533638038-11671
  ### > ,user,hive,warehouse,smy.db,ggsn_hourly_summary,recordopeningtime=20210207

  ### timearr will consists of (represented as comma-delimited array):
  ### > 
  ### > 21,19
  ### > 18,05

  ### datearr will consists of (represented as comma-delimited array):
  ### >
  ### > 2019,05,19
  ### > 2021,08,08
 
  # Filter out any non-directory information from the input 
  if (length(prtarr[n]) > 0) {\
    split(prtarr[n], prt, "=");\
  } else {}; \
  
  # Assuming all the directory listed as input is a partition directory,
  #   we are filtering out any non-partition directory 
  #  (a partition directory is indicated as using = as the separator between partition key & partition value)
  if (length(prt[2]) > 0) {\
    
    ### Expected output of this script from the sample input is:
    ### 
    ### > 2021,08,08,18,05,recordopeningtime,20210207
    ###

    result[1]=datearr[1]
    result[2]=datearr[2]
    result[3]=datearr[3]
    result[4]=timearr[1]
    result[5]=timearr[2]
    result[6]=prt[1]
    result[7]=prt[2]
  
    ### If both -date and -hour argument being passed, filter by hour and date 
    if (length(HRID) > 0 && length(DTID) > 0) {\
      if(timearr[1] == HRID && datearr[1] datearr[2] datearr[3] == DTID) {\
        print_result_by_id(result, RES);\
      } else {};\
    }\

    ### If only -hour argument being passed, filter by hour
    else if (length(HRID) > 0 && length(DTID) == 0) {\
      if(timearr[1] == HRID) {\
        #print datearr[1],datearr[2],datearr[3],timearr[1],timearr[2],prt[1],prt[2];\
        print_result_by_id(result, RES);\
      } else {};\
    }\

    ### If only -date argument being passed, filter by hour
    else if (length(HRID) == 0 && length(DTID) > 0) {\
      if(datearr[1] datearr[2] datearr[3] == DTID) {\
        #print datearr[1],datearr[2],datearr[3],timearr[1],timearr[2],prt[1],prt[2];\
        print_result_by_id(result, RES);\
      } else {};\
    }\

    ### If no arguments at all are passed, show everything
    else {\
      #print datearr[1],datearr[2],datearr[3],timearr[1],timearr[2],prt[1],prt[2];\
      print_result_by_id(result, RES);\
    };\
  } else {};\
}'
