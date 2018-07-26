import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object FIleReaderProducer {
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.56.171:9092"
    val topic = "test"

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG /* 同"key.serializer"必须配置*/,
      classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG /* 同"value.serializer"必须配置*/,
      classOf[StringSerializer].getName)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG /* 同"bootstrap.servers" 必须配置*/, brokers)
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")

    val producer = new KafkaProducer[String, String](props)

    Source.fromFile("src/main/resources/README.md").getLines().foreach { line =>
      println(System.currentTimeMillis())
      producer.send(new ProducerRecord(topic, "key-" + System.currentTimeMillis(), line.toString))
      Thread.sleep(200)
    }

    producer.close()
  }
}
