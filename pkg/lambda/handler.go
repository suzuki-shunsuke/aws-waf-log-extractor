package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/sirupsen/logrus"
)

type Handler struct {
	Firehose                Firehose
	blockDeliveryStreamName *string
	countDeliveryStreamName *string
}

func NewHandler() *Handler {
	sess := session.Must(session.NewSession())
	blockDeliveryStreamName := os.Getenv("BLOCK_DELIVERY_STREAM_NAME")
	countDeliveryStreamName := os.Getenv("COUNT_DELIVERY_STREAM_NAME")
	return &Handler{
		Firehose:                firehose.New(sess),
		blockDeliveryStreamName: aws.String(blockDeliveryStreamName),
		countDeliveryStreamName: aws.String(countDeliveryStreamName),
	}
}

type Firehose interface {
	PutRecordBatchWithContext(ctx aws.Context, input *firehose.PutRecordBatchInput, opts ...request.Option) (*firehose.PutRecordBatchOutput, error)
}

type Event struct {
	Records []*Record `json:"records"`
}

type Record struct {
	RecordID    string `json:"recordId"`
	Result      string `json:"result"`
	Data        string `json:"data"`
	DecodedData Data   `json:"-"`
}

type Data struct {
	Action                      string        `json:"action"`
	NonTerminatingMatchingRules []interface{} `json:"nonTerminatingMatchingRules"`
}

func (handler *Handler) extractRecords(ev *Event) ([]*Record, []*firehose.Record, []*firehose.Record) {
	blockRecords := []*firehose.Record{}
	countRecords := []*firehose.Record{}
	records := make([]*Record, len(ev.Records))
	for i, record := range ev.Records {
		records[i] = &Record{
			RecordID: record.RecordID,
			Result:   "Ok",
			Data:     record.Data,
		}
		decodedData, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"data": record.Data,
			}).WithError(err).Error("decode record.Data by base64")
			continue
		}
		data := Data{}
		if err := json.Unmarshal(decodedData, &data); err != nil {
			logrus.WithFields(logrus.Fields{
				"decoded_data": string(decodedData),
			}).WithError(err).Error("unmarshal data as JSON")
			continue
		}
		if data.Action == "BLOCK" {
			blockRecords = append(blockRecords, &firehose.Record{
				Data: []byte(record.Data),
			})
			continue
		}
		if len(data.NonTerminatingMatchingRules) != 0 {
			countRecords = append(countRecords, &firehose.Record{
				Data: []byte(record.Data),
			})
			continue
		}
	}
	return records, blockRecords, countRecords
}

func (handler *Handler) Do(ctx context.Context, ev *Event) *Event {
	records, blockRecords, countRecords := handler.extractRecords(ev)

	if len(blockRecords) != 0 {
		input := &firehose.PutRecordBatchInput{
			DeliveryStreamName: handler.blockDeliveryStreamName,
			Records:            blockRecords,
		}
		if _, err := handler.Firehose.PutRecordBatchWithContext(ctx, input); err != nil {
			logrus.WithFields(logrus.Fields{
				"delivery_stream_name": aws.StringValue(input.DeliveryStreamName),
				"num_records":          len(input.Records),
			}).WithError(err).Error("put block logs to Firehose")
		}
	}

	if len(countRecords) != 0 {
		input := &firehose.PutRecordBatchInput{
			DeliveryStreamName: handler.countDeliveryStreamName,
			Records:            countRecords,
		}
		if _, err := handler.Firehose.PutRecordBatchWithContext(ctx, input); err != nil {
			logrus.WithFields(logrus.Fields{
				"delivery_stream_name": aws.StringValue(input.DeliveryStreamName),
				"num_records":          len(input.Records),
			}).WithError(err).Error("put count logs to Firehose")
		}
	}

	return &Event{
		Records: records,
	}
}
