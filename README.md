# Process Function API

​	前面学习的Transformations 是无法访问事件的时间戳和水位线信息的，如MapFunction的map转换算子是无法访问时间戳和当前事件的事件时间。基于此，DataStream API提供了一系列的Low Level转换算子--Process Function API，与高层算子不同，通过这些底层转换算子我们可以访问数据的时间戳，watermark以及注册定时事件。Process Function 用来构建事件驱动的应用和实现自定义的业务逻辑。例如Flink SQL就是用Process Function 实现的。

Flink为我们提供了8中process function：

- ProcessFunction
- KeyedProcessFunction
- CoProcessFunction
- ProcessJoinFunction
- BroadcastProcessFunction
- KeyedBroadcastProcessFunction
- ProcessWindowFunction
- ProcessAllWindowFunction

于是我选择了基于Flink的电商用户行为数据分析的项目，进行对Flink的KeyedProcessFunction进行全面学习与认识。

首先我们重点认识下KeyedProcessFunction，KeyedProcessFunction 用来操作 KeyedStream，KeyedProcessFunction 会处理流的每一个元素，KeyedProcessFunction[KEY, IN, OUT]还额外提供了两个方法：

- `processElement(v: IN, ctx: Context, out: Collector[OUT])`, 流中的每一个元素都会调用这个方法，调用结果将会放在 Collector 数据类型中输出。Context可以访问元素的时间戳，元素的 key，以及 TimerService 时间服务。Context还可以将结果输出到别的流(side outputs)。
- `onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT])`是一个回调函数。当之前注册的定时器触发时调用。参数 timestamp 为定时器所设定的触发的时间戳。Collector 为输出结果的集合。OnTimerContext 和 processElement 的 Context 参数一样，提供了上下文的一些信息，例如定时器触发的时间信息(事件时间或者处理时间)。当定时器 timer 触发时，会执行回调函数 onTimer()，注意定时器 timer 只能在keyed streams 上面使用。

# 实时热门页面流量统计

- 基本需求
  - 从 web 服务器的日志中，统计实时的热门访问页面
  - 统计每分钟的 ip 访问量，取出访问量最大的 5 个地址，每 5 秒更新一次
- 解决思路
  - 将 apache 服务器日志中的时间，转换为时间戳，作为 Event Time
  - 构建滑动窗口，窗口长度为 1 分钟，滑动距离为 5 秒

```
import java.text.SimpleDateFormat

import model.ApacheLogEvent
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * 统计近 1 个小时内的热门商品，每5分钟更新一次
 */
object NetWorkFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(5)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val resource = getClass.getResource("/apache.log").getPath
    val inputStream = env.readTextFile(resource)

    val dataStream  = inputStream
      .map(data => {
        val arr = data.split(" ")
        ApacheLogEvent(arr(0), arr(1), new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3)).getTime, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.timestamp
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new NetWorkFlowAggregateFunction, new NetWorkFlowWindowFunction)
      .keyBy(_.windowEnd)
      .process(new NetWorkFlowKeyedProcessFunction(3))
      .print()

    env.execute("NetWorkFlowAnalysis")
  }
}
```

#  实时流量统计—PV 和 UV

- 基本需求
  - 从埋点日志中，统计实时的 PV 和 UV
- 解决思路
  - 统计埋点日志中的 pv 行为，利用 Set 数据结构进行去重

```
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv").getPath
    val inputStream = env.readTextFile(resource)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvAggregateFunction, new PvWindowFunction)
      .keyBy(_.windowEnd)
      .process(new PvKeyedProcessFunction)
      .print()


    env.execute("PageView")
  }
}
```

```
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/UserBehavior.csv").getPath
    val inputStream = env.readTextFile(resource)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvAllWindowFunction)
      .print()

    env.execute()
  }
}
```

#  市场营销分析— APP市场推广计划

- 基本需求
  - 从埋点日志中，统计 APP 市场推广的数据指标
  - 按照不同的推广渠道，分别统计数据
- 解决思路
  - 通过过滤日志中的用户行为数据，按照不同的渠道进行统计
  - 可以用 Process function 处理，得到自定义的输出数据信息

```
object AppMarket {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val sourceStream = env.addSource(new SimulatedSourceFunction).assignAscendingTimestamps(_.timestamp)

    val processStream = sourceStream
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(new AppMarketProcessWindowFunction)
      .print()

    env.execute()
  }
}
```

#  市场营销分析— 页面广告统计

- 基本需求

  - 从埋点日志中，统计每小时页面广告的点击量， 5 秒刷新一次，并按照不同省份进行划分
  - 对于 ”刷单“ 式的频繁点击行为进行过滤，并将该用户加入黑名单

- 解决思路

  - 根据省份进行分组，创建长度为 1 小时、滑动距离为 5 秒的时间窗口进行统计

  - 可以用 process function 进行黑名单过滤，检测用户对同一广告的点击量，

    如果超过上限则将用户信息以测输出流流出到黑名单中

```
object BrushOrderAlert {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/AdClickLog.csv").getPath
    val socketStream = env.readTextFile(resource)
    val dataStream = socketStream
      .map(data => {
        val arr = data.split(",")
        UserAdvertClick(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //将有刷单行为的用户输出到侧输出流（黑名单报警）
    val filterStream = dataStream
      .keyBy(data=>(data.userId, data.advertId))
      .process(new BrushOrderKeyedProcessFunction(100))

    val aggProviceStream = filterStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new BrushOrderAggregateFunction, new BrushOrderWindowFunction)

    aggProviceStream.print("各省份下单详情")
    filterStream.getSideOutput(new OutputTag[BlackList]("BlackList")).print("恶意刷单详情")

    env.execute()
  }
}
```

#  恶意登录监控

- 基本需求
  - 用户在短时间内频繁登录失败，有程序恶意攻击的可能
  - 同一用户（可以是不同 Ip）在 2 秒内连续两次登录失败，需要报警
- 解决思路
  - 将用户的登录失败行为存入 ListState，设定定时器 2 秒后触发，查看 ListState 中有几次失败登录
  - 更加精确的检测，可以使用 CEP 库实现事件流的模式匹配

```
object ContinuousLoginFailure {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/LoginLog.csv").getPath
    val socketStream = env.readTextFile(resource)
    val dataStream = socketStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: LoginEvent): Long = t.timestamp * 1000L
      })

    val loginStream = dataStream
      .keyBy(_.userId)
      .process(new LoginFailAdvanceKeyedProcessFunction(2))

    dataStream.print("LoginEvent");
    loginStream.print("LoginFailWarning")

    env.execute()
  }
}
```

# 订单支付实时监控

- 基本需求
  - 用户下单之后，应设置订单失效时间，以提高用户支付的意愿，并降低系统风险
  - 用户下单后 15 分钟未支付，则输出监控信息
- 解决思路
  - 利用 CEP 库进行事件流的模式匹配，并设定匹配的时间间隔
  - 也可以利用状态编程，用 process function 实现处理逻辑

```
object OrderTimeoutWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv").getPath
    val inputStream = env.readTextFile(resource)

    val orderEventStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //带时间限制的pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .followedBy("follow")
        .where(_.eventType.equals("pay"))
          .within(Time.minutes(15))


    //将pattern作用到inputStream上，得到一个patternStream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    val orderTimeoutTag = OutputTag[OrderResult]("orderTimeoutTag")
    //调用select，对超时的做测流输出报警
    val resultStream = patternStream.select(orderTimeoutTag, new OrderTimeOutPatternTimeoutFunction, new OrderPayPatternSelectFunction)

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutTag).print("timeout")

    env.execute()
  }
}
```

# 侧输出流（SideOutput）

大部分的 DataStream API 的算子的输出是单一输出，也就是某种数据类型的流。processfunction 的 side outputs 功能可以产生多条流，并且这些流的数据类型可以不一样。一个 side output 可以定义为 OutputTag[X]对象，X 是输出流的数据类型。processfunction 可以通过 Context 对象发射一个事件到一个或者多个 sideoutputs。

```
/**
 * 1s之内温度连续上升就报警
 */
object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val socketStream = env.socketTextStream("localhost", 9998)
    val dataStream = socketStream
      .map(data => {
        val arr = data.split(",")
        TempReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TempReading](Time.seconds(1)) {
        override def extractTimestamp(t: TempReading): Long = {t.timestamp + 1000L}
      })

    val processStream = dataStream
      .keyBy(_.id)
      .process(new TempKeyedProcessFunction)


    dataStream.print("data")
    processStream.getSideOutput(new OutputTag[String]("alert")).print("output")
    env.execute()
  }
}
```

我把代码上传到我的github上，具体代码请移步：[flink_behavior_analysis](https://github.com/ShawnVanorGit/flink_behavior_analysis)
