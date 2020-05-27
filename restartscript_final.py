import shutil
import fnmatch
from datetime import datetime
import subprocess
import os


def find_file_by_extension(path, pattern):
  files_list = []
  for root, dirs, files in os.walk(path):
    
    for basename in files:

      if fnmatch.fnmatch(basename, pattern):

        file_path = os.path.abspath(os.path.join(root, basename))
        if "bkp" not in basename.split("_"):
          if os.path.isfile(file_path):
            file_info = {"base_path":root , "file_path":file_path ,"file_name":basename }
            files_list.append(file_info)
  return files_list


def get_date_stamp_name(name):
  current_date = str(datetime.now())
  date_splits = current_date.split(" ")
  date_info = date_splits[0].split("-")
  date_info.reverse()
  date_info= "".join(date_info)
  time_info = "".join(date_splits[1].split(".")[0].split(":"))
  basename = name.split(".failed")[0]
  suffix = basename + "_bkp_" + date_info + "_" + time_info + ".failed"
  return suffix


def run_sub_process(shell_script):
  response = []
  try:
    process = subprocess.Popen(shell_script,shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                           universal_newlines=True)
    while True:
      output = process.stdout.readline()
      response.append(output.strip())
      return_code = process.poll()
      if return_code is not None:
          # print('RETURN CODE', return_code)
          for output in process.stdout.readlines():
            response.append(output.strip())
            print(output.strip())
          break
  except subprocess.CalledProcessError as e:
    print(e)
  return response

def call_sub_process(shell_script):
  process = subprocess.call(shell_script, shell=True)


def handle_failed_rename(batch_folders):
  print("Started :  Handle Failed Rename")
  failed_files_list = []

  for folder_path in batch_folders:
    if os.path.exists(folder_path):
      failed_files = find_file_by_extension(folder_path, "*.failed")
      # print(failed_files)
      failed_files_list.extend(failed_files)
  print(failed_files_list)
  for file_info in failed_files_list:
    path = file_info["file_path"]
    base_path = file_info["base_path"]
    file_name = file_info["file_name"]

    new_name = get_date_stamp_name(file_name)
    base_name = file_name.split(".failed")[0]

    # Copy as backup
    shutil.copyfile(path, base_path + "/" +  new_name)
    # Rename file
    os.rename(path,base_path+ "/" + base_name)


def kill_process(pid):
  call_sub_process("kill -9 " + pid)
  print("Process " + pid + " killed!")

def kill_limit_manager(keyword , patternList):
  output = run_sub_process("ps -ef|grep " + keyword)
  # print("Output ->" , output)
  for out in output:
    tokens  =  out.split(" ")
    match_result =  all(item in tokens for item in patternList)
    if match_result:
      pid = tokens[5]
      kill_process(pid)

def handle_tomcat_restart():
  print("Started : Tomcat Restart")
  # stop script
  TOMCAT_PATH  = "/opt/tomcat/bin"
  TOMCAT_STARTUP = TOMCAT_PATH + "/startup.sh"
  TOMCAT_STOP = TOMCAT_PATH + "/shutdown.sh"
  call_sub_process(TOMCAT_STOP)
  call_sub_process(TOMCAT_STARTUP)

  print("Done :  Tomcat Restart")
 

def handle_limter_restart():

  print("Started :  Limter  Restart ")
  # stop script
  kill_limit_manager("LimitsMngr" , ["-jar" , "TriggerListener"])
  
  #start script
  SCRIPT_PATH = "/opt/ldm3/batch"
  LISTENER_SCRIPT = SCRIPT_PATH + "/startLimitsMngrTriggerListener.sh"
  LIMITSMNGR_SCRIPT = SCRIPT_PATH + "/StartLimitsMngr.sh &"

  call_sub_process(LISTENER_SCRIPT)
  call_sub_process(LIMITSMNGR_SCRIPT)

  print("Done :  Limter  Restart ")



def main():
  batch_folders = ['/opt/ldm3/batch/data' , '/opt/ldm3/batch/triggers']
  # Rename failed files
  handle_failed_rename(batch_folders)
  # Stop and Start Tomcat
  handle_tomcat_restart()
  # Restart Listener
  handle_limter_restart()


if __name__ == '__main__':
   main()
