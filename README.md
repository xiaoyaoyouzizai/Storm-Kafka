# Storm-Kafka

This is a test Storm-Kafka performance project.

flow:
kafka-console-producer -> kafka1 -> ExtractFieldsBolt -> kafka2 -> SumOrdersBolt -> kafka3 -> kafka-console-consumer

zcat 20151001_1_orders_hgame.gz|head -n 100000|awk '{ match($0, / VALUES \((.+)\);/, arr); if(arr[1] != "") print arr[1] }' | kafka-console-producer --broker-list 192.168.1.143:9092 --topic orders

kafka-console-consumer --zookeeper 192.168.1.141:2181 --topic orders
