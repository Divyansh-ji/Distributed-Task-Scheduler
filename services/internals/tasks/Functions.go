package tasks

import (
	"context"
	"fmt"
)

func SendEmail(ctx context.Context, payload string) error {
	fmt.Println("Hey this is your email", payload)
	return nil
}

func GenerateReport(ctx context.Context, payload string) error {
	fmt.Println("The report generated", payload)
	return nil
}

//There is a reason why we are not usng the gin.context here because gin should only be limited to http boundaries and in the business logic should be free from any Kind of framework so here we are using normal contex which is provided by go
