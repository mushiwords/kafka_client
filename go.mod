module kafka_client

go 1.13

require (
	github.com/Shopify/sarama v1.27.1
	github.com/bsm/sarama-cluster v2.1.15+incompatible
)

replace github.com/Shopify/sarama => github.com/Shopify/sarama v1.26.4
