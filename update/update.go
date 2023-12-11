package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"log"
)

type Payment struct {
	PartitionKey     string  `dynamodbav:"pk"`
	Operation        string  `dynamodbav:"operation"`
	CreditCardType   string  `dynamodbav:"credit_card_type"`
	CreditCardNumber string  `dynamodbav:"credit_card_number"`
	CreditCardCvv    string  `dynamodbav:"credit_card_cvv"`
	CreditCardExp    string  `dynamodbav:"credit_card_exp"`
	Price            float64 `dynamodbav:"price"`
	Currency         string  `dynamodbav:"currency"`
}

const TABLE string = "ledger-update-poc"
const REGION string = "us-west-2"

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(REGION))
	if err != nil {
		log.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg)

	fmt.Println("enter the number of requests:")

	var n int
	if _, err := fmt.Scanln(&n); err != nil {
		log.Fatal(err)
	}

	addCredit(n, client)
}

func addCredit(n int, client *dynamodb.Client) {
	var payments []Payment
	for i := 0; i < n; i++ {
		payment := Payment{
			PartitionKey:     gofakeit.UUID(),
			Operation:        "DEBIT",
			CreditCardType:   gofakeit.CreditCardType(),
			CreditCardNumber: gofakeit.CreditCardNumber(nil),
			CreditCardCvv:    gofakeit.CreditCardCvv(),
			CreditCardExp:    gofakeit.CreditCardExp(),
			Price:            gofakeit.Price(1000000, 0),
			Currency:         gofakeit.CurrencyShort(),
		}

		payments = append(payments, payment)

		if len(payments) == 25 || i == n-1 {
			writeBatch(client, payments)
			updatePriceToNegative(client, payments)
			payments = []Payment{} // Reset do slice para o próximo lote
		}
	}
}

func updatePriceToNegative(client *dynamodb.Client, payments []Payment) {
	for _, payment := range payments {
		key, err := attributevalue.MarshalMap(map[string]string{
			"pk": payment.PartitionKey,
		})
		if err != nil {
			log.Fatal(err)
		}

		update := "SET price = :p"
		exprAttrValues, err := attributevalue.MarshalMap(map[string]float64{
			":p": -abs(payment.Price), // assegura que o preço é negativo
		})
		if err != nil {
			log.Fatal(err)
		}

		input := &dynamodb.UpdateItemInput{
			TableName:                 aws.String(TABLE),
			Key:                       key,
			UpdateExpression:          aws.String(update),
			ExpressionAttributeValues: exprAttrValues,
		}

		_, err = client.UpdateItem(context.Background(), input)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func writeBatch(client *dynamodb.Client, payments []Payment) {
	requestItems := make([]types.WriteRequest, len(payments))
	for i, payment := range payments {
		av, err := attributevalue.MarshalMap(payment)
		if err != nil {
			log.Fatal(err)
		}

		requestItems[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: av,
			},
		}
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			TABLE: requestItems,
		},
	}

	_, err := client.BatchWriteItem(context.Background(), input)
	if err != nil {
		log.Fatal(err)
	}
}
