package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/brianvoe/gofakeit/v6"
	"log"
	"sync"
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

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg)

	fmt.Println("enter the number of requests:")

	var n int
	if _, err := fmt.Scanln(&n); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go addCredit(&wg, n, client)

	wg.Add(1)
	go addDebit(&wg, n, client)

	wg.Wait()
}

func addDebit(wg *sync.WaitGroup, n int, client *dynamodb.Client) {
	defer wg.Done()

	var payments []Payment
	for i := 0; i < n; i++ {
		payment := Payment{
			PartitionKey:     gofakeit.UUID(),
			Operation:        "DEBIT",
			CreditCardType:   gofakeit.CreditCardType(),
			CreditCardNumber: gofakeit.CreditCardNumber(nil),
			CreditCardCvv:    gofakeit.CreditCardCvv(),
			CreditCardExp:    gofakeit.CreditCardExp(),
			Price:            gofakeit.Price(-1000000, 0),
			Currency:         gofakeit.CurrencyShort(),
		}

		payments = append(payments, payment)

		if len(payments) == 25 || i == n-1 {
			writeBatch(client, payments, "ledger-poc")
			payments = []Payment{} // Reset do slice para o próximo lote
		}
	}
}

func addCredit(wg *sync.WaitGroup, n int, client *dynamodb.Client) {
	defer wg.Done()

	n2 := n * 2

	var payments []Payment
	for i := 0; i < n2; i++ {
		payment := Payment{
			PartitionKey:     gofakeit.UUID(),
			Operation:        "CREDIT",
			CreditCardType:   gofakeit.CreditCardType(),
			CreditCardNumber: gofakeit.CreditCardNumber(nil),
			CreditCardCvv:    gofakeit.CreditCardCvv(),
			CreditCardExp:    gofakeit.CreditCardExp(),
			Price:            gofakeit.Price(1, 1000000),
			Currency:         gofakeit.CurrencyShort(),
		}

		payments = append(payments, payment)

		if len(payments) == 25 || i == n2-1 {
			writeBatch(client, payments, "ledger-poc")
			payments = []Payment{} // Reset do slice para o próximo lote
		}
	}
}

func writeBatch(client *dynamodb.Client, payments []Payment, tableName string) {
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
			tableName: requestItems,
		},
	}

	_, err := client.BatchWriteItem(context.Background(), input)
	if err != nil {
		log.Fatal(err)
	}
}
