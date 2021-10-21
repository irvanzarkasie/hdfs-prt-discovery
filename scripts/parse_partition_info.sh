## Parse script arguments
while [ ! -z "$1" ]; do
  case "$1" in
     -hour)
         shift
         HRID=${1}
         ;;
     *)
  esac
shift
done

### SAMPLE INPUT:
### [START OF FILE]
###  Found 213 items
###  drwxrwx--x+  - hive hive          0 2019-05-19 21:19 /user/hive/warehouse/smy.db/ggsn_hourly_summary/.hive-staging_hive_2019-05-19_10-17-20_912_339861419533638038-11671
###  drwxrwx--x+  - hive hive          0 2021-08-08 18:05 /user/hive/warehouse/smy.db/ggsn_hourly_summary/recordopeningtime=20210207
### [END OF FILE]

cat smy_ggsn_hdfs |\
awk -v HRID="${HRID}" -v DTID="${DTID}" '{\

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

    if (length(HRID) > 0) {\
      if(timearr[1] == HRID) {\
        print datearr[1],datearr[2],datearr[3],timearr[1],timearr[2],prt[1],prt[2];\
      } else {};\
    } else {\
      print datearr[1],datearr[2],datearr[3],timearr[1],timearr[2],prt[1],prt[2];\
    };\
  } else {};\
}'
