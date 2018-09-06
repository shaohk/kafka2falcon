package main

/*
采集kafka topic的offset信息，采集consumer offset信息；采集consumer lags
*/

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"net/http"
	"os"
	"strings"
	"time"
)

//import "time"
//参数解析
var (
	ParameterEndpoint      string
	ParameterBrokers       string
	ParameterConsumer      string
	ParameterTopic         string
	ParameterGroup         string
	ParameterFalconUrl     string
	ParameterEnableFalcon  bool
	ParameterEnableConsole bool
)

type TopicInfo struct {
	Name      string `json:"name"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (this *TopicInfo) ToString() string {
	return fmt.Sprintf("topic=%s,partition=%d", this.Name, this.Partition)
}

type ConsumerInfo struct {
	ConsumerGroup string `json:"name"`
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	Lags          int64  `json:"offset"`
}

func (this *ConsumerInfo) ToString() string {
	return fmt.Sprintf("consumergroup=%s,topic=%s,partition=%d", this.ConsumerGroup, this.Topic, this.Partition)
}

func returnErr(err string) {
	fmt.Fprintf(os.Stderr, err)
	os.Exit(2)
}

func main() {

	flag.StringVar(&ParameterEndpoint, "endpoint", "", "falcon endpoint, eg:data_kafka_cluster")
	flag.StringVar(&ParameterBrokers, "brokers", "127.0.0.1:9092", "Broker list, eg:127.0.0.1:9092,127.0.0.1:9092, default is 127.0.0.1:9092")
	flag.StringVar(&ParameterConsumer, "consumer", "", "Consumer group list, eg:consumer1,consumer2, default is nil")
	flag.StringVar(&ParameterTopic, "topic", "", "Topic list, eg:topic1,topic2, default is nil")
	flag.StringVar(&ParameterGroup, "group", "", "Department group, eg:sys")
	flag.StringVar(&ParameterFalconUrl, "falconurl", "", "falcon agent url, eg:http://127.0.0.1:1988/v1/push")
	flag.BoolVar(&ParameterEnableFalcon, "enable.falcon", false, "enable send monitor data to falcon agent api, eg:true")
	flag.BoolVar(&ParameterEnableConsole, "enable.console", false, "print monitor data to console, eg:true")

	flag.Parse()

	//if ParameterEndpoint == "" {
	//	return
	//}

	//调用sarama client
	conf := sarama.NewConfig()
	conf.Metadata.RefreshFrequency = 0
	conf.Version = sarama.V0_11_0_0

	brokersList := strings.Split(ParameterBrokers, ",")
	client, err := sarama.NewClient(brokersList, conf)
	if err != nil {
		returnErr("failed to create client to connect broker")
	}
	defer client.Close()
	if err := client.RefreshMetadata(); err != nil {
		returnErr(fmt.Sprintf("failed to refresh meta: %s", err))
	}

	//getTopicConsumerGroupInfo(client)
	topicInfo, consumerInfo, consumerAggInfo, err := getTopicConsumerGroupInfo(client)
	if err != nil {
		returnErr(fmt.Sprintf("%s", err))
	}

	var data []map[string]interface{}
	for _, topic := range topicInfo {
		dt := make(map[string]interface{})
		dt["endpoint"] = ParameterEndpoint
		dt["metric"] = "topic.offset"
		dt["value"] = topic.Offset
		dt["timestamp"] = time.Now().Unix()
		dt["step"] = 180
		dt["counterType"] = "COUNTER"
		dt["tags"] = topic.ToString()
		data = append(data, dt)
	}

	for _, consumer := range consumerInfo {
		dt := make(map[string]interface{})
		dt["endpoint"] = ParameterEndpoint
		dt["metric"] = "consumer.lags"
		dt["value"] = consumer.Lags
		dt["timestamp"] = time.Now().Unix()
		dt["step"] = 180
		dt["counterType"] = "GAUGE"
		dt["tags"] = consumer.ToString()
		data = append(data, dt)
	}

	for consumer, topicMap := range consumerAggInfo {
		for topic, lagssum := range topicMap {
			dt := make(map[string]interface{})
			dt["endpoint"] = ParameterEndpoint
			dt["metric"] = "consumer.lagsSum"
			dt["value"] = lagssum
			dt["timestamp"] = time.Now().Unix()
			dt["step"] = 180
			dt["counterType"] = "GAUGE"
			dt["tags"] = fmt.Sprintf("consumer=%s,topic=%s", consumer, topic)
			data = append(data, dt)
		}
	}

	if ParameterGroup != "" {
		for _, dt := range data {
			dt["tags"] = fmt.Sprintf("%s,group=%s", dt["tags"], ParameterGroup)
		}
	}

	if ParameterEnableFalcon {
		jsonData, err := json.Marshal(data)
		//fmt.Println(string(jsonData))

		var url string
		if ParameterFalconUrl == "" {
			url = "http://10.100.20.225:1988/v1/push"
		} else {
			url = ParameterFalconUrl
		}
		res, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(string(jsonData))))
		if err != nil {
			fmt.Println("http request error:", err)
		}
		defer res.Body.Close()
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		httpClient := &http.Client{}
		resp, err := httpClient.Do(req)
		if err != nil {
			fmt.Println("response Status:", err)
		}
		defer resp.Body.Close()
	}
}

func map2str(m map[string]string) string {
	str := ""
	for k, v := range m {
		if str == "" {
			str = fmt.Sprintf("%s=%s", k, v)
		} else {
			str = fmt.Sprintf("%s,%s=%s", str, k, v)
		}
	}
	return str
}

//return topicOffsetMap consumerGroupOffsetMap consumerGroupLagsMap
func getTopicConsumerGroupInfo(client sarama.Client) ([]*TopicInfo, []*ConsumerInfo, map[string]map[string]int64, error) {
	var topicOffsetList []*TopicInfo
	var consumerGroupInfoList []*ConsumerInfo
	var consumerAggInfo map[string]map[string]int64 //map[consumer][topic]lagssum

	var groupList, topics []string

	if ParameterTopic == "" {
		_topics, err := client.Topics()
		if err != nil {
			fmt.Printf("failed to invoke client.Topics:%s\n", err)
			return nil, nil, nil, err
		}
		topics = _topics
	} else {
		topics = strings.Split(ParameterTopic, ",")
	}

	if ParameterConsumer == "" {
		groupList = getGroupList(client)
	} else {
		groupList = strings.Split(ParameterConsumer, ",")
	}

	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			fmt.Println("failed to invoke client.Partitions:%s\n", err)
			return nil, nil, nil, err
		}

		for _, pt := range partitions {

			logOffset, err := client.GetOffset(topic, pt, sarama.OffsetNewest)
			if err != nil {
				fmt.Printf("failed to get topic %s partition %d offset, err: %s\n", topic, pt, err)
				return nil, nil, nil, err
			}

			if logOffset == 0 {
				continue
			}

			topicOffsetList = append(topicOffsetList, &TopicInfo{
				Name:      topic,
				Partition: pt,
				Offset:    logOffset,
			})

			//consumerGroupPtLags := make(map[string]int)
			for _, group := range groupList {
				offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
				if err != nil {
					fmt.Printf("failed to invoke client.Topics:%s\n", err)
					return nil, nil, nil, err
				}

				partitionOffsetManager, err := offsetManager.ManagePartition(topic, pt)
				if err != nil {
					fmt.Printf("failed to invoke offsetManager.ManagePartition:%s\n", err)
					return nil, nil, nil, err
				}
				consumerOffset, _ := partitionOffsetManager.NextOffset()

				partitionOffsetManager.Close()
				offsetManager.Close()

				if consumerOffset != -1 {
					consumerGroupInfoList = append(consumerGroupInfoList, &ConsumerInfo{
						ConsumerGroup: group,
						Topic:         topic,
						Partition:     pt,
						Lags:          logOffset - consumerOffset,
					})
					if consumerMap, ok := consumerAggInfo[group]; ok {
						if lagsSum, ok := consumerMap[topic]; ok {
							consumerAggInfo[group][topic] = lagsSum + logOffset - consumerOffset
						} else {
							consumerAggInfo[group][topic] = logOffset - consumerOffset

						}
					} else {
						consumerAggInfo[group] = map[string]int64{topic: logOffset - consumerOffset}
					}
				}
			}
		}
	}
	return topicOffsetList, consumerGroupInfoList, consumerAggInfo, nil
}

func getGroupList(client sarama.Client) []string {
	var groupList []string
	for _, broker := range client.Brokers() {
		err := broker.Open(client.Config())
		if err != nil {
			fmt.Printf("open broker failed err: %s\n", err)
		}

		listGroupReq := new(sarama.ListGroupsRequest)
		listGroupRes, err := broker.ListGroups(listGroupReq)
		if err != nil {
			fmt.Printf("failed to invoke broker.ListGroups:%s\n", err)
			continue
		}

		for groupk, _ := range listGroupRes.Groups {
			groupList = append(groupList, groupk)
		}
		if broker != nil {
			broker.Close()
		}
	}
	fmt.Println(groupList)
	return groupList
}
