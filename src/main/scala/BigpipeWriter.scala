package app.ecom.dmp.search

import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.Date
import scala.annotation.tailrec
import com.baidu.mcpack.Mcpack
import com.baidu.gson.JsonObject
import com.baidu.bigpipe.framework.Message
import com.baidu.bigpipe.framework.NonblockingPublisher
import com.baidu.bigpipe.framework.Producer
import com.baidu.bigpipe.framework.PublishEventListener
import com.baidu.bigpipe.impl.BigpipeNonblockWriter
import com.baidu.bigpipe.meta.ZooKeeperUtil
import com.baidu.bigpipe.protocol.BigpipeProtocol.AckCommand
import com.baidu.bigpipe.protocol.BigpipeProtocol.ConnectedCommand

case class BigpipeConf(master: String, cluster: String, pipeName: String, pipeNums: Int, userName: String, passWord: String, startPoint: Long)

object BigpipeWriter {
  val queue = new LinkedBlockingQueue[String](8000)
  println("write to queue *******************maxdebug\n")

  def write(message: String): Unit = {
    queue.put(message)
  }

  println("link to nova-zk.dmop.baidu.com:2181 *******************maxdebug\n")
  try {
    ZooKeeperUtil.init("nova-zk.dmop.baidu.com:2181", 2000)
  } catch {
    case e: Exception => println(e)
  }
  ZooKeeperUtil.getInstance.waitConnected
  val conf = BigpipeConf(
    "nova-zk.dmop.baidu.com:2181",
    "BIGPIPE_NOVA_LOG_CLUSTER",
    "online-dmp-id-uid-pipe",
    100,
    "uap-dmp",
    "uap-dmp",
    -1
  )
  val createBigpipeWriter = new CreateBigpipeWriter
  println("create queue *******************maxdebug\n")
  for (pipeNum <- 1 to conf.pipeNums)
    yield createBigpipeWriter.create(queue, conf.copy(pipeNums = pipeNum))
  println("create thread *******************maxdebug\n")
  new Thread(createBigpipeWriter).start
}

class CreateBigpipeWriter extends Runnable {
  val publisher: NonblockingPublisher = new NonblockingPublisher

  def create(queue: LinkedBlockingQueue[String], bigpipeConf: BigpipeConf) = {
    val writer: BigpipeNonblockWriter = new BigpipeNonblockWriter(bigpipeConf.cluster)
    writer.setUsername(bigpipeConf.userName)
    writer.setPassword(bigpipeConf.passWord)
    writer.setPipeletName("%s_%s".format(bigpipeConf.pipeName, bigpipeConf.pipeNums))
    writer.setId("%s_%s_%s".format(InetAddress.getLocalHost.toString, bigpipeConf.pipeNums.toString, System.currentTimeMillis.toString))
    val producer = new Producer
    producer.setWriter(writer)
    publisher.addProducer(producer)
    val bigpipeWriterThread = new BigpipeWriterThread(queue, producer)
    new Thread(bigpipeWriterThread).start
  }

  override def run(): Unit = {
    try {
      publisher.select();
    } catch {
      case e: Exception =>
        println(e)
    }
  }
}

class BigpipeWriterThread(queue: LinkedBlockingQueue[String], producer: Producer) extends Runnable with PublishEventListener {
  val pack = new Mcpack
  val jsonMessage = new JsonObject
  val count = new AtomicInteger
  producer.setPublishEventListener(this)

  override def run(): Unit = {
    try {
      println("bigpipe thread start *******************maxdebug\n")
      while (true) {
        queue.take match {
          case message: String =>
            count.get > 500 match {
              case true =>
                while (count.get > 500) {
                  println("bigpipe write while get>500 *******************maxdebug\n")
                  Thread.sleep(100)
                }
              case false =>
            }
            send(message)
            count.incrementAndGet
            println("increment ,when ACK decrement,maxdebug message:%s *******************maxdebug\n".format(message))
        }
      }
    } catch {
      case e: Exception =>
        println("start to print exception when write bigpipe *******************maxdebug\n")
        println(e)
        println("end of print exception when write bigpipe *******************maxdebug\n")
    }
    println("bigpipe thread end*******************maxdebug\n")
  }

  def toMcpack(message: String): Array[Byte] = {
    val date = new Date()
    jsonMessage.addProperty("body", message)
    jsonMessage.addProperty("type", 0)
    jsonMessage.addProperty("body_type", 0)
    jsonMessage.addProperty("msg_time", date.getTime / 1000)
    pack.toMcpack("gb18030", jsonMessage)
  }

  @tailrec
  private def send(message: String): Unit = {
    producer.send(toMcpack(message)) match {
      case true =>
      case false =>
        Thread.sleep(100)
        send(message)
    }
  }

  override def onSessionConnected(response: ConnectedCommand): Unit = {
    println("%s %s *******************maxdebug\n".format(producer.getWriter.getPipeletName, "connected"))
  }

  override def onAcked(msg: Message, ack: AckCommand): Unit = {
    println("ACK!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!maxdebug")
    count.decrementAndGet
  }

  override def onClose(): Unit = {
    println("%s %s *******************maxdebug\n".format(producer.getWriter.getPipeletName, "close"))
  }
}