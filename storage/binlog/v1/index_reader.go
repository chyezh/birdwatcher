package binlogv1

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type IndexReader struct{}

func NewIndexReader(f *os.File) (*IndexReader, DescriptorEvent, error) {
	reader := &IndexReader{}
	var de DescriptorEvent
	var err error

	_, err = ReadMagicNumber(f)
	if err != nil {
		return nil, de, err
	}

	de, err = ReadDescriptorEvent(f)
	if err != nil {
		return nil, de, err
	}
	return reader, de, err
}

func (reader *IndexReader) NextEventReader(f *os.File, dataType schemapb.DataType) ([][]byte, error) {
	eventReader := NewEventReader()
	header, err := eventReader.ReadHeader(f)
	if err != nil {
		return nil, err
	}
	ifed, err := ReadIndexFileEventData(f)
	if err != nil {
		return nil, err
	}

	next := header.EventLength - header.GetMemoryUsageInBytes() - ifed.GetEventDataFixPartSize()
	data := make([]byte, next)
	io.ReadFull(f, data)

	pr, err := NewParquetPayloadReader(dataType, data)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	switch dataType {
	case schemapb.DataType_String:
		result, err := pr.GetStringFromPayload(0)
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		return lo.Map(result, func(data string, _ int) []byte {
			return []byte(data)
		}), nil
	case schemapb.DataType_Int8:
		result, err := pr.GetBytesFromPayload(0)
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		return [][]byte{result}, nil
	}
	return nil, errors.New("unexpected data type")
}
