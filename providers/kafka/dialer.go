package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func NewDialer() *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}
}

func NewSecureDialer(awsRegion string) (*kafka.Dialer, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(awsRegion))
	if err != nil {
		return nil, fmt.Errorf("unable to create secure dialer: %w", err)
	}
	d := NewDialer()
	d.SASLMechanism = aws_msk_iam_v2.NewMechanism(cfg)
	d.TLS = &tls.Config{}
	return d, nil
}

func transportFromDialer(dialer *kafka.Dialer) *kafka.Transport {
	return &kafka.Transport{
		DialTimeout: dialer.Timeout,
		IdleTimeout: dialer.Timeout,
		SASL:        dialer.SASLMechanism,
		TLS:         dialer.TLS,
	}
}
