# canal-kafka-client
> 此client是为canal(公网mysql增量更新服务)开发而来
## 此client支持两种增量更新方式
1. 直接订阅canal server的实例
```
use_kafka_not_use_subscribe: False  # 设置使用从kafka接受数据还是从canal server接受数据

subscribe_config: # 配置
  host: "172.22.22.192" # canal 服务ip
  port: 11111   # canal 服务端口
  username: "canal" # master数据库用户名
  password: "canal" # master数据库密码
  client_id: 1001   # 标记客户端id
  destination: "test"   # canal服务实例名称，可以理解为kafka主题，进行订阅
  sleep_time: 1 # 每次拉取消息的时间间隔
  get_count: 100    # 每次拉取消息的最大数量
  thread_pool_count: 20 # 线程池数量，提升处理并发量
```

2. 从kafka接受数据
```
use_kafka_not_use_subscribe: True  # 设置使用从kafka接受数据还是从canal server接受数据

kafka_config:
  address:
    - "172.22.22.192:9092"  # kafka服务地址
  group_id: 1   # 消费者group id
  topic:
    - topic_name: "example" # 主题名称
      partition:
        number: 0   # 哪个分区partition
        offset: # 设置消费的offset，不设置从最新的地方开始
  thread_pool_count: 20
```

* 可以订阅多个主题多个分区，配置实例
```
kafka_config:
  address:
    - "172.22.22.192:9092"  # kafka服务地址
  group_id: 1   # 消费者group id
  topic:
    - topic_name: "example" # 主题名称
      partition:
        number: 0   # 哪个分区partition
        offset: # 设置消费的offset，不设置从最新的地方开始
     - topic_name: "example" # 主题名称
      partition:
        number: 1   # 哪个分区partition
        offset: # 设置消费的offset，不设置从最新的地方开始
    - topic_name: "test" # 主题名称
      partition:
        number: 0   # 哪个分区partition
        offset: # 设置消费的offset，不设置从最新的地方开始
  thread_pool_count: 20
```

## 架构
> 此client基于观察者模式开发，便于在接收消息的同时，增加多个观察者实现不同的行为，目前的只有一个观察者(向数据库同步数据)，
> 如果有其他的业务需求，请继承Observer，重写update接口
```
class KafkaObserver(Observer, ABC):
    def __init__(self, subject: Subject):
        self.subject = subject
        self.subject.attach(self)

    def update(self, message: dict):
        """
        定义具体的行为
        """
        sql = KafkaObserver.parser_message_to_mysql(message)
        KafkaObserver.exec_mysql(sql)
```