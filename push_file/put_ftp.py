# -*- coding: utf-8 -*-
from subprocess import Popen, PIPE
import os
import shutil
import traceback
from log_tool import toollogger
from send_mail import SendMail

def get_domain():
    domains = []
    with open("/data1/hry/xinlan_domain") as isf:
        for line in isf.readlines():
            splits = line.split(",")
            if splits[6]=="3038":
                 domains.append(splits[0])
    #print domains, len(domains)
    return domains

def get_status_output(cmd):
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    output, err = p.communicate()
    return p.returncode, output, err

# shell 命令最多重试3次
def get_status_output_with_retry(cmd):
    max_retry = 3
    while True:
        max_retry -= 1
        status, output, err = get_status_output(cmd)
        if status == 0:
            return status, output, err
        else:
            if max_retry == 0:
                return status, output, err

def join_path(*paths):
    return os.path.join(*paths)

def is_file_exist(f):
    return os.path.isfile(f)

def is_path_exist(path):
    return os.path.isdir(path)

def get_file_size(file_path):
    return os.path.getsize(file_path)

def mkdir_is_not_exist(path):
    if not is_path_exist(path):
        os.makedirs(path)

# 目录下的所有文件名
def get_recursive_file_list(path):
    current_files = os.listdir(path)
    all_files = []
    for file_name in current_files:
        full_file_name = join_path(path, file_name)
        if is_path_exist(full_file_name):
            next_level_files = get_recursive_file_list(full_file_name)
            all_files.extend(next_level_files)
        else:
            all_files.append(full_file_name)
    return all_files 

def push_file_ftp(file_path, domain_path, day_path, ftp_path):
    cmd = """echo "open ip
                 prompt
                 user ** ***
                 mkdir  {domain_path}
                 cd  {domain_path}
                 mkdir  {day_path}
                 cd  {day_path}
                 binary
                 put {file_path} {ftp_path}
                 close
                 bye" |ftp -v -n  >>run.log """.format(
        file_path=file_path,
        domain_path=domain_path,
        day_path=day_path,
        ftp_path=ftp_path
    )
    try:
        status, output, err = get_status_output_with_retry(cmd)
        # 获取推送到FTP服务器文件的大小
        with open("/data1/hry/run.log") as asf:
            line = asf.readlines()[-2]
            size = line.split(" ")[0]
            ftp_size = size if "bytes sent in" in line else "0"
        return ftp_size, cmd
    except Exception:
        toollogger.error(traceback.format_exc())
        return "0", cmd


def run(day_format):
    #day format like 2017-10-23
    year, month, day= day_format.split("-")
    domains = get_domain()
    for domain in domains:
        base_dir = "/usr/local/nginx/html/hdfs/download/"
        hdfs_path = domain + "/" + year + "/" + month + "/" + day
        local_path = base_dir + hdfs_path
        if is_path_exist(local_path):
            all_files = get_recursive_file_list(local_path)
            for file_path in all_files:
                local_size = get_file_size(file_path)
                hour = file_path.split("/")[-1]
                domain_path = "/"+domain
                day_path = "./" + day_format
                ftp_path = "/"+domain + "/" + day_format + "/" + day_format+"_"+hour
                ftp_size, cmds = push_file_ftp(file_path, domain_path, day_path, ftp_path)
                if ftp_size=="0":
                    toollogger.error(traceback.format_exc())
                    SendMail().send_mail(day_format, cmds)
                else:
                    toollogger.info(
                        ftp_path + "\t" + str(local_size) + "\t" + ftp_size + "\t" +str(int(local_size) - int(ftp_size)))
        
if __name__ == '__main__':
    if len(sys.argv) == 2:
        _, day_format = tuple(sys.argv)
    else:
        print("param error")
    run(day_format)

"""
date=`date -d'-1 day' +"%Y-%m-%d"`
echo $date
cd /data1/hry
python push_2ftp.py $date
"""