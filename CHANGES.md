crawlframe-1.0.3
==========================
下载器并发方式变更



crawlframe-1.0.2
==========================
```
CRAWLFRAME_SURVIVE_SWITCH = True
CRAWLFRAME_SURVIVE_MAX = int
reload spider
```
CRAWLFRAME_SURVIVE_SWITCH = True
CRAWLFRAME_SURVIVE_MAX = int
设置两个参数
发出一定请求数之后, 自动重启爬虫. 避免内存溢出.
修复middle不能加载以及logger不能创建的bug.
BaseLogger改为单例模式.


crawlframe-1.0.1
==========================
``` 
crawlf createapp <appname> -url http://<website>/
crawlf createpro <projectname>
crawlf start app:<name>
crawlf start project
crawlf stop app:<name>
crawlf stop project
crawlf stop <pid>
``` 

实际上以上命令只实现了start 与 stop. 
但是已经能够在项目中使用了.
为了方便自己安装使用, 就先上传到 pypi 与 github 了.