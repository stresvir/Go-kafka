package main

import (
    "fmt"
    "net/http"
    "strconv"
    "time"

    "github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
    PORT       = 8080
    Topic      = "helloworld"
    GroupID    = "golang"
    Offset     = "earliest"
    BrokerAddr = "localhost:9092" // Change this to your Kafka broker address if different
)

func main() {
    go startKafkaProducer()
    startHTTPServer()
}

func startKafkaProducer() {
    config := &kafka.ConfigMap{
        "bootstrap.servers": BrokerAddr,
    }

    producer, err := kafka.NewProducer(config)
    if err != nil {
        fmt.Printf("Failed to create producer: %s\n", err)
        return
    }
    defer func() {
        fmt.Println("Closing producer")
        producer.Close()
    }()

    topic := Topic // Create a variable from the constant

    for i := 0; i < 10; i++ {
        fmt.Printf("Sending message to Kafka %d\n", i)

        key := strconv.Itoa(i)
        value := fmt.Sprintf("Hello %d", i)
        msg := &kafka.Message{
            TopicPartition: kafka.TopicPartition{
                Topic:     &topic,
                Partition: kafka.PartitionAny,
            },
            Key:   []byte(key),
            Value: []byte(value),
        }

        err := producer.Produce(msg, nil)
        if err != nil {
            fmt.Printf("Failed to produce message: %s\n", err)
        }
    }

    fmt.Println("Flushing producer")
    producer.Flush(5 * 1000)
}

func startHTTPServer() {
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintln(w, "Hello, World!")
    })

    fmt.Printf("Listening on port %d\n", PORT)

    err := http.ListenAndServe(fmt.Sprintf(":%d", PORT), nil)
    if err != nil {
        fmt.Printf("Failed to start the server: %s\n", err)
    }
}

func startKafkaConsumer() {
    config := &kafka.ConfigMap{
        "bootstrap.servers": BrokerAddr,
        "group.id":          GroupID,
        "auto.offset.reset": Offset,
    }

    consumer, err := kafka.NewConsumer(config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    err = consumer.Subscribe(Topic, nil)
    if err != nil {
        panic(err)
    }

    for {
        message, err := consumer.ReadMessage(1 * time.Second)
        if err == nil {
            fmt.Printf("Receive message : %s\n", message.Value)
        }
    }
}
