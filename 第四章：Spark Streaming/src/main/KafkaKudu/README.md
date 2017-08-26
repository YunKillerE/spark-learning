# 主要功能

实现从kafka读取数据写入kudu的一个通用可配置的工具

# 版本

* java 1.8
* scala 2.11.8
* cdh 5.11.0

# 实现思路

1. 读取properties文件配置，包含topics、分区数、zookeeper、brokens、kudu表结构、kudu表名等等,这里会有个问题，
2. 两种读取kafka的api，什么情况使用，怎么使用性能高，注释里面写清楚技术要点，理解kafka manager中的所有选项以及建topic的技术点
3. spark streaming过程处理，怎么处理高效，怎么提升性能，注释里面写清楚技术要点
4. 写入kudu，每次接受一个DF，可以考虑将建表也加入进来，如果不调用impala的api建表在impala中会看不到
5. 同时加一个功能，写入hdfs

# 








