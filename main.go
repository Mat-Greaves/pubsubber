package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	"cloud.google.com/go/pubsub"
)

func main() {
	var topicID = os.Args[1]
	r := regexp.MustCompile(`^projects\/([^\/]*)\/topics\/([^\/]*)$`)
	res := r.FindStringSubmatch(topicID)
	if len(res) == 0 {
		log.Fatal("Expected input of the form: 'projects/{projecID}/topics/{topidID}")
	}
	project := res[1]
	topic := res[2]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	// Run context cancellation in a goroutine as the call to subscribeAndListen will block until
	// the context is cancelled.
	go func() {
		<-sigs
		fmt.Println("Shutting down...")
		cancel()
	}()
	err := subscribeAndListen(ctx, project, topic)
	if err != nil {
		log.Fatal(err)
	}
}

// subscribeAndListen creates a subscription to the requested topic and prints messages
// received to stdout. This function blocks until ctx is cancelled, after which it will
// clean up the created subscription and return
func subscribeAndListen(ctx context.Context, project string, topic string) error {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client: %w", err)
	}
	hexString, err := randomHexString(5)
	if err != nil {
		return fmt.Errorf("failed to generate random identifier: %w", err)
	}
	sub, err := client.CreateSubscription(ctx, fmt.Sprintf("pubsubber-%s", hexString), pubsub.SubscriptionConfig{Topic: client.Topic(topic)})
	if err != nil {
		return fmt.Errorf("failed to to create subscription: %w", err)
	}
	config, _ := sub.Config(ctx)
	defer sub.Delete(context.Background())
	fmt.Printf("Listening for messages on: %s\n", config.Topic)
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		fmt.Println(m.Attributes)
		fmt.Println(string(m.Data))
		m.Ack()
	})
	if err != nil {
		return fmt.Errorf("failed to set up subscription reciever: %w", err)
	}
	return nil
}

func randomHexString(n int) (string, error) {
	bytes := make([]byte, 5)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
