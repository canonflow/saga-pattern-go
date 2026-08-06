.PHONY: run-choreography run-orchestration

run-choreography:
	go run choreography/services/order/main.go

run-orchestration:
	go run orchestration/cmd/api/main.go