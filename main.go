package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

// Topic TODO
type Topic struct {
	Topic             string            `json:"topic"`
	ReplicationFactor int32             `json:"replication_factor"`
	Partitions        int32             `json:"partitions"`
	Config            map[string]string `json:"config"`
}

func main() {
	// Parse command line arguments
	kafkaAddr := flag.String("kafkaAddr", "", "Kafka address")
	username := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	jsonFile := flag.String("jsonFile", "", "Path to JSON file")
	flag.Parse()

	// Read and parse JSON file
	file, err := os.ReadFile(*jsonFile)
	if err != nil {
		fmt.Println("Please specify a file. And --help for usage.", err)
		os.Exit(1)
	}

	var topics []Topic
	err = json.Unmarshal(file, &topics)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		os.Exit(1)
	}

	// Configure sarama
	config := sarama.NewConfig()
	config.Net.SASL.Enable = true
	config.Net.SASL.User = *username
	config.Net.SASL.Password = *password
	config.Metadata.Full = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &XDGSCRAMClient{
			HashGeneratorFcn: SHA512}
	}
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512

	// Create new sarama client
	client, err := sarama.NewClient([]string{*kafkaAddr}, config)
	if err != nil {
		fmt.Println("Error creating client:", err)
		os.Exit(1)
	}

	// Create or update topics
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		fmt.Println("Error creating cluster admin:", err)
		os.Exit(1)
	}

	for _, topic := range topics {
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     topic.Partitions,
			ReplicationFactor: int16(topic.ReplicationFactor),
			ConfigEntries:     make(map[string]*string),
		}

		for key, value := range topic.Config {
			v := value
			topicDetail.ConfigEntries[key] = &v
		}

		topicDetails, err := admin.DescribeTopics([]string{topic.Topic})
		if err != nil {
			fmt.Println("Error describing topic:", err)
			os.Exit(1)
		}
		if topicDetails[0].Err == sarama.ErrUnknownTopicOrPartition { // If topic does not exist, create it
			err = admin.CreateTopic(topic.Topic, topicDetail, false)
			if err != nil {
				fmt.Println("Error creating topic:", err)
				os.Exit(1)
			}
			fmt.Printf("Topic[%s] created.\n", topic.Topic)
		} else {
			// If topic exists, update its config
			fmt.Printf("Topic[%s] existed. trying to update it \n", topic.Topic)
			entries := make(map[string]*string)
			for key, value := range topic.Config {
				v := value
				entries[key] = &v
			}
			err = admin.AlterConfig(sarama.TopicResource, topic.Topic, entries, false)
			if err != nil {
				fmt.Println("Error updating topic config:", err)
				os.Exit(1)
			}
			if int32(len(topicDetails[0].Partitions)) < topic.Partitions {
				err = admin.CreatePartitions(topic.Topic, topic.Partitions, nil, false)
				if err != nil {
					fmt.Println("Error increasing partitions:", err)
					os.Exit(1)
				}
				fmt.Printf("Increased partitions of topic[%s] to %d.\n", topic.Topic, topic.Partitions)
			}

		}
	}
	fmt.Println("Finished.")
}
