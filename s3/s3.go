package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
)

var client *s3.Client

func Init() error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}
	client = s3.NewFromConfig(cfg)
	return nil
}

func Get(bucket string, key string) ([]byte, error) {
	params := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	res, err := client.GetObject(context.TODO(), &params)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	content, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func Put(bucket string, key string, body io.Reader) error {
	params := s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:	body,
	}
	_, err := client.PutObject(context.TODO(), &params)
	if err != nil {
		return err
	}
	return nil
}