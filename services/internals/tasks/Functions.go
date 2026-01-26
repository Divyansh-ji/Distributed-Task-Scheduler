package tasks

import (
	"DistributedTaskScheduler/services/internals/api"
	"context"
	"fmt"
)

func SendEmail(ctx context.Context, payload api.CreateTaskRequest) error {
	email := payload
	fmt.Println("Hey this is your email", email)
	return nil
}

func GenerateReport(ctx context.Context, payload api.CreateTaskRequest) error {
	report := payload
	fmt.Println("The report generated", report)
	return nil
}

//There is a reason why we are not usng the gin.context here because gin should only be limited to http boundaries and in the business logic should be free from any Kind of framework so here we are using normal contex which is provided by go
