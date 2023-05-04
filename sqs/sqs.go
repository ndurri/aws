package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Message struct {
	Attributes MessageAttributes
	Body *string
	ReceiptHandle *string
}

type MessageAttributes map[string]string
type SDKMessageAttributes map[string]types.MessageAttributeValue

var client *sqs.Client

func Init() error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}
	client = sqs.NewFromConfig(cfg)
	return nil
}

func attributesToSDKAttributes(attributes MessageAttributes) SDKMessageAttributes {
	ma := SDKMessageAttributes{}

	for key, value := range attributes {
		ma[key] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(value),
		}
	}
	return ma
}

func sdkAttributesToAttributes(sdkAttributes SDKMessageAttributes) MessageAttributes {
	ma := MessageAttributes{}

	for key, value := range sdkAttributes {
		ma[key] = *value.StringValue
	}
	return ma	
}

func Get(queue string) (*Message, error) {
	params := sqs.ReceiveMessageInput{
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(queue),
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   int32(20),
		WaitTimeSeconds:	 int32(30),
	}
	res, err := client.ReceiveMessage(context.TODO(), &params)
	if err != nil {
		return nil, err
	}
	if len(res.Messages) < 1 {
		return nil, nil
	} else {
		message := res.Messages[0]
		return &Message{
			Attributes: sdkAttributesToAttributes(message.MessageAttributes),
			Body: message.Body,
			ReceiptHandle: message.ReceiptHandle,
		}, nil
	}
}

func Put(queue string, body string, attributes MessageAttributes) error {
	params := sqs.SendMessageInput{
		DelaySeconds: 0,
		MessageAttributes: attributesToSDKAttributes(attributes),
		MessageBody: aws.String(body),
		QueueUrl:    aws.String(queue),
	}
	_, err := client.SendMessage(context.TODO(), &params)
	return err
}