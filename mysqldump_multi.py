# -*- coding: utf-8 -*-

import os
import time
import multiprocessing
import sys


# 用法
# nohup python mysqldump_multi 40 /export/backup/dapbackup_not_del/10_127_160_166/ >>/export/scripts/edison/nohup.out &


# 第一个参数为进程数，第二参数为解压文件所在的路径
class auto_source(object):
    def __init__(self, para1, para2):
        self.source_mark = True
        self.store_logfile = "/export/scripts/edison/source_sql.log"
        self.store_file = "/export/scripts/edison/"
        self.err_logfile = self.store_file + "source_sql.err"

        self.tasknumber = int(para1)  # 进程数
        self.filename = ""
        self.file_argv = para2

        self.MySQL_Port = 3358
        self.MySQL_User = "admin"
        self.MySQL_Password = "11111111111"
        self.MySQL_Config_Path = "/export/servers/mysql/etc/my.cnf"
        self.server_ip = "1.1.1.1"

        self.MySQL_Connect_TimeOut = 5
        self.MySQL_Charset = "utf8"

        self.sql_count = 0
        self.sql_list = []
        self.success_count = 0
        self.failed_list = []

        try:
            dirs = os.listdir(self.file_argv)
            for files in dirs:
                if files[-3:] != "log":
                    self.filename = self.file_argv + '/' + files
        except Exception as e:
            os.system("echo \"{0}\"".format(e) + " >> " + self.err_logfile)

        self.filename = self.filename.replace(r"//", r"/")
        self.Mysql_Database_Name = "report_" + self.filename.split("/")[-2]

        # 得到的self.filename 的格式为/export/backup/dapbackup_not_del/1_1_1_1/2018-07-20

    # 获取所有的sql文件,并且按文件大小排序
    def get_file_name(self, path):
        file_list_tmp = []
        all_file_size = {}
        for files in os.walk(path):
            file_list_tmp.append(files)
        file_list = file_list_tmp[0][2]
        for files in file_list:
            if files[-3:] == "sql":
                self.sql_count += 1
                all_file_size[files] = int(os.path.getsize(self.filename + "/" + files) / 1024)
        sorted_list = sorted(all_file_size.items(), key=lambda x: x[1])
        return sorted_list

    # 根据进程数 分割要处理的文件，并且尽可能地让每一个进程处理相同大小的任务，达到均衡
    def devide_file(self, path):
        file_list_all = self.get_file_name(path)
        sub_file = []
        for i in range(self.tasknumber):
            sub_file.append([])

        for i in range(len(file_list_all)):
            sub_file[i % self.tasknumber].append(self.filename + "/" + file_list_all[i][0])
        for i in range(len(sub_file)):
            if i % 2 == 0:
                sub_file[i].reverse()

        return sub_file

    # 准备工作
    def pre(self):
        os.system(">" + "/export/scripts/edison/nohup.out")
        cmd_mysql = "mysql -u{user} -p{password}  -e \"show databases;\"".format(user=self.MySQL_User,
                                                                                 password=self.MySQL_Password
                                                                                 )
        cmd = cmd_mysql + " | grep {0}".format(self.Mysql_Database_Name)
        recode = os.system(cmd)
        if recode != 0:
            self.create_db()
        cmd = "mysql -u{user} -p{password}  -e \"reset master;\"".format(user=self.MySQL_User,
                                                                         password=self.MySQL_Password,
                                                                         db_name=self.Mysql_Database_Name)

        os.system(cmd)
    #创建数据库
    def create_db(self):
        cmd = "mysql -u{user} -p{password}  -e \"create database {db_name}\"".format(user=self.MySQL_User,
                                                                                     password=self.MySQL_Password,
                                                                                     db_name=self.Mysql_Database_Name)
        os.system(cmd)

    # 解压
    def start_source(self):
        dir_name = self.filename.split("/")[-2]
        os.system("echo \"{0}\"".format(dir_name) + " >> " + self.store_logfile)
        self.write_time("start source:")
        sub_file_lists = self.devide_file(self.filename)
        for i in range(self.tasknumber):
            time.sleep(1)
            sub_task = multiprocessing.Process(target=self.exec_cmd(sub_file_lists[i]))
            sub_task.start()
        for i in range(self.tasknumber):
            sub_task.join()
        if self.source_mark:
            self.write_time("stop source:")
        else:
            os.system("echo \"source failed!See the error log in {0}\"".format(
                self.err_logfile) + " >> " + self.store_logfile)

        os.system("echo \"\"  >> " + self.store_logfile)

    # 开始运行
    def exec_cmd(self, sqllist):

        for sqlname in sqllist:
            cmd = "mysql -u{user} -p{password}  -e \"reset master;\"".format(user=self.MySQL_User,
                                                                             password=self.MySQL_Password,
                                                                             db_name=self.Mysql_Database_Name)

            os.system(cmd)
            cmd = "mysql -u{user} -p{password}  {db_name} < {file_local}".format(user=self.MySQL_User,
                                                                                 password=self.MySQL_Password,
                                                                                 file_local=sqlname,
                                                                                 db_name=self.Mysql_Database_Name)
            recode = os.system(cmd)
            if recode != 0:
                self.failed_list.append(sqlname)
            else:
                self.success_count += 1

    # 往日志中写时间
    def write_time(self, pre):
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        os.system("echo \"{prestr}{0}\"".format(current_time, prestr=pre) + " >> " + self.store_logfile)

    # 检查source sql文件是否成功，否则重新导入
    def check(self):
        process_list = []
        failed_number = 5  # 失败次数
        if len(self.failed_list)!=0:
            os.system(r"echo -e '  \n\n\n'>>{0}".format( self.err_logfile))
            os.system(r"echo  '{0}'>>{1}".format(self.Mysql_Database_Name,self.err_logfile))

        while len(self.failed_list) != 0:
            os.system(
                "echo \"{0} source error!Already source again\"  >> {1}".format(
                    self.failed_list[0], self.err_logfile))

            cmd = "mysql -u{user} -p{password}  -e \"reset master;\"".format(user=self.MySQL_User,
                                                                             password=self.MySQL_Password,
                                                                             db_name=self.Mysql_Database_Name)
            os.system(cmd)

            cmd = "mysql -u{user} -p{password}  {db_name} < {file_local}".format(user=self.MySQL_User,
                                                                                 password=self.MySQL_Password,
                                                                                 file_local=self.failed_list[0],
                                                                                 db_name=self.Mysql_Database_Name)
            recode = os.system(cmd)
            if recode == 0:
                self.success_count += 1
                self.failed_list.pop(0)

            else:
                process_list.append(self.failed_list[0])
                if process_list.count(self.failed_list[0]) >= failed_number:  # 若循环解压单个超过5次，请手动source，否则陷入死循环
                    os.system(
                        "echo \"{0} unzip error.Please unzip in person\"  >> {1}".format(
                            self.failed_list[0], self.err_logfile))
                    self.failed_list.pop(0)
                    self.source_mark = False


if __name__ == '__main__':
    ins = auto_source(sys.argv[1], sys.argv[2])
    ins.pre()
    ins.start_source()
    ins.check()
