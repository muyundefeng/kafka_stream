package com.jasongj.kafka.stream.producer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.jasongj.kafka.producer.HashPartitioner;
import com.jasongj.kafka.stream.model.User;
import com.jasongj.kafka.stream.serdes.GenericSerializer;

public class UserProducer {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:19092");
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", GenericSerializer.class.getName());
		props.put("value.serializer.type", User.class.getName());
		props.put("partitioner.class", HashPartitioner.class.getName());//发送消息到那个分区日志中。

		Producer<String, User> producer = new KafkaProducer<String, User>(props);
		List<User> users = readUser();
		users.forEach((User user) -> producer.send(new ProducerRecord<String, User>("users", user.getName(), user)));
		producer.close();
	}
	
	public static List<User> readUser() throws IOException {
		//从文件中读取用户数据
		List<String> lines = IOUtils.readLines(OrderProducer.class.getResourceAsStream("/users.csv"), Charset.forName("UTF-8"));
		List<User> users = lines.stream()
				//.filter(line->StringUtils.isNoneBlank(line))
			.filter(StringUtils::isNoneBlank)//进行循环操作，循环遍历每个元素。进行元素过滤
			.map((String line) -> line.split("\\s*,\\s*"))//函数式编程中实现(lines元素中)值的类型之间的转化，string->string[]。
			.filter((String[] values) -> values.length == 4)//进行过滤操作
			.map((String[] values) -> new User(values[0], values[1], values[2], Integer.parseInt(values[3])))
			.collect(Collectors.toList());
		return users;
	}

}
