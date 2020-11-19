几个demo例子：

- 消费Wikipedia实时消息。wiki-edits项目
- 交易欺诈检测。DataStream API 。frauddetection项目
- table api。spend-report。这里还有流处理的方式。





需要注意的问题，我这里的maven依赖，都把 `<scope>provided</scope>` 的注释去掉了，因为是直接在本地intellij IDE中执行的。provided表示该依赖在打包过程中，不需要打进去，这个由运行的环境来提供，比如tomcat或者基础类库等等，事实上，该依赖可以参与编译、测试和运行等周期，与compile等同。区别在于打包阶段进行了exclude操作。





其他可以尝试的：

flink elasticsearch的例子 https://www.elastic.co/blog/building-real-time-dashboard-applications-with-apache-flink-elasticsearch-and-kibana





flink获取数据源这块，和mq consumer类似，有什么区别么？



