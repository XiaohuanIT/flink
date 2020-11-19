flink的基本例子。

（1）SocketWindowWordCountJava

这里，需要先在命令行窗口启动执行 `nc -l 9001` 再运行此代码，否则会报错。


疑问1： `nc -l 9001` 不应该是监听 9001的端口么，怎么变成了向这个端口发送数据？

疑问2： 对于flink，与hadoop是啥关系。我安装的1.10.1，是没有看到与hadoop的继承版本。

疑问3：生成的jar，为啥不能通过flink的web界面正常执行呢？

疑问4：udf



(2) 

批量操作的例子



（3）Flink SQL

FlinkSqlDemo1 的例子，直接使用POJO，使用了元组，但是这样其实不是个好习惯，无形中增加了编程的复杂度。

输出结果：

```
(1,C-罗纳尔多,尤文图斯,26,0,19,5,7,111,61)
(2,夸利亚雷拉,桑普多利亚,26,0,19,5,5,76,42)
(3,萨帕塔,亚特兰大,26,0,16,1,4,53,31)
(4,米利克,那不勒斯,26,0,14,0,1,61,34)
(5,皮亚特克,热那亚,19,0,13,2,0,56,31)
(6,因莫比莱,拉齐奥,24,0,12,3,3,65,35)
(7,卡普托,恩波利,26,0,12,2,4,47,28)
(8,帕沃莱蒂,卡利亚里,23,0,10,0,1,44,22)
(9,佩塔尼亚,斯帕尔,25,0,10,2,0,44,29)
(10,热尔维尼奥,帕尔马,21,0,9,0,0,21,15)
(11,伊卡尔迪,国际米兰,23,0,9,3,2,44,23)
```



但是会有异常信息输出：

```
[2020-06-03 11:15:16 AM]:DEBUG org.apache.flink.runtime.taskexecutor.TaskExecutor.freeSlotInternal(TaskExecutor.java:1500)Could not free slot for allocation id 053b17ce3adb62eaedb35bb6de98ad2e.
org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException: Could not find slot for 053b17ce3adb62eaedb35bb6de98ad2e.
	at org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl.freeSlot(TaskSlotTableImpl.java:367)
```





参考：
1、https://medium.com/@eranga.h.n/say-hello-to-flink-3592043505d8

2、https://mp.weixin.qq.com/s?__biz=MzI3MDU3OTc1Nw==&mid=2247483991&idx=1&sn=c5ff5c1a2fab19798f40d2242763a0ee&chksm=eacfa315ddb82a03a98b73cc36a4c9b4cbe62a78b631a0bb744593c1d6c5a07c18fa289e8486&scene=21#wechat_redirect

