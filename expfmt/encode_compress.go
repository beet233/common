package expfmt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	dto "github.com/prometheus/client_model/go"
)

const (
	initialCompressedBufferSize = 1024
)

type Metadata struct {
	Version                uint64
	MetricFamilyMap        map[uint64]MetricFamilyMetadata
	ReverseMetricFamilyMap map[string]uint64
}

type MetricFamilyMetadata struct {
	MetricType      dto.MetricType
	Name            string
	Help            string
	LabelMap        map[uint64]string
	ReverseLabelMap map[string]uint64
}

type CompressEncoder struct {
	metadata Metadata
}

var encoder *CompressEncoder = &CompressEncoder{Metadata{MetricFamilyMap: make(map[uint64]MetricFamilyMetadata), ReverseMetricFamilyMap: make(map[string]uint64)}}

func GetCompressEncoder() *CompressEncoder {
	return encoder
}

func (encoder *CompressEncoder) Encode(mfs []*dto.MetricFamily, latestMetadataVersion uint64, reqMetadataVersion uint64, out io.Writer) (err error) {
	if latestMetadataVersion != encoder.metadata.Version {
		// update encoder.metadata
		// TODO 注意一下锁？
		// clear original map
		encoder.metadata.Version = latestMetadataVersion
		encoder.metadata.MetricFamilyMap = make(map[uint64]MetricFamilyMetadata)
		encoder.metadata.ReverseMetricFamilyMap = make(map[string]uint64)
		for index, metricFamily := range mfs {
			metadata := MetricFamilyMetadata{metricFamily.GetType(), metricFamily.GetName(), metricFamily.GetHelp(), make(map[uint64]string), make(map[string]uint64)}
			// 后续 lazy 地处理 LabelMap
			encoder.metadata.MetricFamilyMap[uint64(index)] = metadata
			encoder.metadata.ReverseMetricFamilyMap[metadata.Name] = uint64(index)
		}
	}
	// encode
	// 先写在 buffer 里，如果有 metadata，可以让 metadata 先写入 out，方便接收端解析
	w := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
	// Try the interface upgrade. If it doesn't work, we'll use a
	// bufio.Writer from the sync.Pool.
	// w, ok := out.(enhancedWriter)
	// if !ok {
	// 	b := bufPool.Get().(*bufio.Writer)
	// 	b.Reset(out)
	// 	w = b
	// 	defer func() {
	// 		bErr := b.Flush()
	// 		if err == nil {
	// 			err = bErr
	// 		}
	// 		bufPool.Put(b)
	// 	}()
	// }
	// MAGIC_NUM
	_, err = w.WriteString("cprval")
	if err != nil {
		return
	}
	// VERSION
	_, err = writeRawInt(w, encoder.metadata.Version)
	if err != nil {
		return
	}
	// METRIC_FAMILY_LEN
	_, err = writeRawInt(w, uint64(len(mfs)))
	if err != nil {
		return
	}
	for _, metricFamily := range mfs {
		_, err = writeRawInt(w, encoder.metadata.ReverseMetricFamilyMap[metricFamily.GetName()])
		if err != nil {
			return
		}
		_, err = writeRawInt(w, uint64(len(metricFamily.GetMetric())))
		if err != nil {
			return
		}
		metricType := metricFamily.GetType()
		metricFamilyMetadata := encoder.metadata.MetricFamilyMap[encoder.metadata.ReverseMetricFamilyMap[metricFamily.GetName()]]
		for _, metric := range metricFamily.GetMetric() {
			// write label length
			_, err = writeRawInt(w, uint64(len(metric.GetLabel())))
			if err != nil {
				return
			}
			// write label pairs
			for _, label := range metric.GetLabel() {
				if _, ok := metricFamilyMetadata.ReverseLabelMap[label.GetName()]; !ok {
					// update label pairs into metadata
					metricFamilyMetadata.LabelMap[uint64(len(metricFamilyMetadata.LabelMap))] = label.GetName()
					metricFamilyMetadata.ReverseLabelMap[label.GetName()] = uint64(len(metricFamilyMetadata.ReverseLabelMap))
				}
				_, err = writeRawInt(w, metricFamilyMetadata.ReverseLabelMap[label.GetName()])
				if err != nil {
					return
				}
				_, err = writeRawInt(w, uint64(len(label.GetValue())))
				if err != nil {
					return
				}
				_, err = w.WriteString(label.GetValue())
				if err != nil {
					return
				}
			}
			switch metricType {
			case dto.MetricType_COUNTER:
				_, err = writeRawFloat(w, metric.GetCounter().GetValue())
				if err != nil {
					return
				}
			case dto.MetricType_GAUGE:
				_, err = writeRawFloat(w, metric.GetGauge().GetValue())
				if err != nil {
					return
				}
			case dto.MetricType_UNTYPED:
				_, err = writeRawFloat(w, metric.GetUntyped().GetValue())
				if err != nil {
					return
				}
			case dto.MetricType_SUMMARY:
				for _, quantile := range metric.GetSummary().GetQuantile() {
					_, err = writeRawFloat(w, quantile.GetQuantile())
					if err != nil {
						return
					}
					_, err = writeRawFloat(w, quantile.GetValue())
					if err != nil {
						return
					}
				}
				_, err = writeRawInt(w, metric.GetSummary().GetSampleCount())
				if err != nil {
					return
				}
				_, err = writeRawFloat(w, metric.GetSummary().GetSampleSum())
				if err != nil {
					return
				}
			case dto.MetricType_HISTOGRAM:
				for _, bucket := range metric.GetHistogram().GetBucket() {
					_, err = writeRawFloat(w, bucket.GetUpperBound())
					if err != nil {
						return
					}
					_, err = writeRawFloat(w, float64(bucket.GetCumulativeCount()))
					if err != nil {
						return
					}
				}
				_, err = writeRawInt(w, metric.GetHistogram().GetSampleCount())
				if err != nil {
					return
				}
				_, err = writeRawFloat(w, metric.GetHistogram().GetSampleSum())
				if err != nil {
					return
				}
			default:
				return fmt.Errorf(
					"unexpected type in metric %s %s", metricFamily.GetName(), metric,
				)
			}
		}
	}
	fmt.Printf("reqMetadataVersion: %v, holdingMetadataVersion: %v\n", reqMetadataVersion, encoder.metadata.Version)
	if reqMetadataVersion != encoder.metadata.Version {
		// write new metadata
		w := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
		// MAGIC_NUM
		_, err = w.WriteString("cprmeta")
		if err != nil {
			return
		}
		// VERSION
		_, err = writeRawInt(w, encoder.metadata.Version)
		if err != nil {
			return
		}
		// METRIC_FAMILY_LEN
		_, err = writeRawInt(w, uint64(len(encoder.metadata.MetricFamilyMap)))
		if err != nil {
			return
		}
		for i := 0; i < len(encoder.metadata.MetricFamilyMap); i++ {
			metricFamilyMetadata := encoder.metadata.MetricFamilyMap[uint64(i)]
			_, err = writeRawInt(w, uint64(metricFamilyMetadata.MetricType))
			if err != nil {
				return
			}
			_, err = writeRawInt(w, uint64(len(metricFamilyMetadata.Name)))
			if err != nil {
				return
			}
			_, err = w.WriteString(metricFamilyMetadata.Name)
			if err != nil {
				return
			}
			_, err = writeRawInt(w, uint64(len(metricFamilyMetadata.Help)))
			if err != nil {
				return
			}
			_, err = w.WriteString(metricFamilyMetadata.Help)
			if err != nil {
				return
			}
			for j := 0; j < len(metricFamilyMetadata.LabelMap); j++ {
				_, err = writeRawInt(w, uint64(len(metricFamilyMetadata.LabelMap[uint64(j)])))
				if err != nil {
					return
				}
				_, err = w.WriteString(metricFamilyMetadata.LabelMap[uint64(j)])
				if err != nil {
					return
				}
			}
		}
		_, err = w.WriteTo(out)
		if err != nil {
			return
		}
	}
	_, err = w.WriteTo(out)
	return err
}

// 因为 w 已经是先写到内存的 buffer 了，这里不需要再申请 buffer
func writeRawFloat(w enhancedWriter, f float64) (int, error) {
	err := binary.Write(w, binary.LittleEndian, f)
	if err != nil {
		return 0, err
	}
	return int(reflect.TypeOf(f).Size()), err
}

// 写入原始的 int 编码
// TODO 改为 LEB128 编码
func writeRawInt(w enhancedWriter, value uint64) (int, error) {
	// err := binary.Write(w, binary.LittleEndian, i)
	// if err != nil {
	// 	return 0, err
	// }
	// return int(reflect.TypeOf(i).Size()), err
	written := 0
	for i := 0; i < 8; i++ {
		// 取最低7位
		b := byte(value & 0x7F)
		value >>= 7
		if value != 0 {
			// 如果还有剩余的值，设置最高位为1
			b |= 0x80
		}
		err := w.WriteByte(b)
		if err != nil {
			return 0, err
		}
		written += 1
		if value == 0 {
			break
		}
	}
	// 如果还有非 0 值，说明还需要填充一个字节的数据，最后 8 bit 直接写入
	if value != 0 {
		b := byte(value & 0xFF)
		err := w.WriteByte(b)
		if err != nil {
			return 0, err
		}
		written += 1
	}
	return written, nil
}
