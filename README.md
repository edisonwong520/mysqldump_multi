## 多进程并发mysqldump导入工具      
1. 原理  
自定义进程数  
根据进程数把要导入的sql文件分块，并且对sql文件大小进行排序，打乱，借此尽可能实现每个进程要处理相同任务量
执行任务，如有导入异常，重新导入，超过5次失败录入到error文件并退出导入


2. 使用说明     
安装基于python 3.6 开发，linux环境。需要按 MySQLdb 库。   
nohup python mysqldump_multi 40 /export/backup/dapbackup_not_del/10_127_160_166/ >>/export/scripts/edison/nohup.out &

3. 优势
   能把io或cpu跑满

4. 备注  
   个人日常小工具
