package orchestrator

import "context"

type Orchestrator struct{}

func (orches *Orchestrator) StartSaga(ctx context.Context) {
	go consumePaymentReplies(ctx)
	go consumeShippingReplies(ctx)

	<-ctx.Done()
}

func (orchers *Orchestrator) SendCommand(context context.Context, topic string)

func consumePaymentReplies(ctx context.Context)
func consumeShippingReplies(ctx context.Context)
