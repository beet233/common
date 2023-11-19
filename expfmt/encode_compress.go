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
	version                uint64
	metricFamilyMap        map[uint64]MetricFamilyMetadata
	reverseMetricFamilyMap map[string]uint64
}

type MetricFamilyMetadata struct {
	metricType      dto.MetricType
	name            string
	help            string
	labelMap        map[uint64]string
	reverseLabelMap map[string]uint64
}

type CompressEncoder struct {
	metadata Metadata
}

var encoder *CompressEncoder = &CompressEncoder{Metadata{metricFamilyMap: make(map[uint64]MetricFamilyMetadata), reverseMetricFamilyMap: make(map[string]uint64)}}

func GetCompressEncoder() *CompressEncoder {
	return encoder
}

func (encoder *CompressEncoder) Encode(mfs []*dto.MetricFamily, latestMetadataVersion uint64, reqMetadataVersion uint64, out io.Writer) (err error) {
	if latestMetadataVersion != encoder.metadata.version {
		// update encoder.metadata
		// TODO 注意一下锁？
		// clear original map
		encoder.metadata.version = latestMetadataVersion
		encoder.metadata.metricFamilyMap = make(map[uint64]MetricFamilyMetadata)
		encoder.metadata.reverseMetricFamilyMap = make(map[string]uint64)
		for index, metricFamily := range mfs {
			metadata := MetricFamilyMetadata{metricFamily.GetType(), metricFamily.GetName(), metricFamily.GetHelp(), make(map[uint64]string), make(map[string]uint64)}
			// 后续 lazy 地处理 labelMap
			encoder.metadata.metricFamilyMap[uint64(index)] = metadata
			encoder.metadata.reverseMetricFamilyMap[metadata.name] = uint64(index)
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
	_, err = writeRawInt(w, encoder.metadata.version)
	if err != nil {
		return
	}
	// METRIC_FAMILY_LEN
	_, err = writeRawInt(w, uint64(len(mfs)))
	if err != nil {
		return
	}
	for _, metricFamily := range mfs {
		_, err = writeRawInt(w, encoder.metadata.reverseMetricFamilyMap[metricFamily.GetName()])
		if err != nil {
			return
		}
		_, err = writeRawInt(w, uint64(len(metricFamily.GetMetric())))
		if err != nil {
			return
		}
		metricType := metricFamily.GetType()
		metricFamilyMetadata := encoder.metadata.metricFamilyMap[encoder.metadata.reverseMetricFamilyMap[metricFamily.GetName()]]
		for _, metric := range metricFamily.GetMetric() {
			// write label length
			_, err = writeRawInt(w, uint64(len(metric.GetLabel())))
			if err != nil {
				return
			}
			// write label pairs
			for _, label := range metric.GetLabel() {
				if _, ok := metricFamilyMetadata.reverseLabelMap[label.GetName()]; !ok {
					// update label pairs into metadata
					metricFamilyMetadata.labelMap[uint64(len(metricFamilyMetadata.labelMap))] = label.GetName()
					metricFamilyMetadata.reverseLabelMap[label.GetName()] = uint64(len(metricFamilyMetadata.reverseLabelMap))
				}
				_, err = writeRawInt(w, metricFamilyMetadata.reverseLabelMap[label.GetName()])
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
	fmt.Printf("reqMetadataVersion: %v, holdingMetadataVersion: %v\n", reqMetadataVersion, encoder.metadata.version)
	if reqMetadataVersion != encoder.metadata.version {
		// write new metadata
		w := bytes.NewBuffer(make([]byte, 0, initialCompressedBufferSize))
		// MAGIC_NUM
		_, err = w.WriteString("cprmeta")
		if err != nil {
			return
		}
		// VERSION
		_, err = writeRawInt(w, encoder.metadata.version)
		if err != nil {
			return
		}
		// METRIC_FAMILY_LEN
		_, err = writeRawInt(w, uint64(len(encoder.metadata.metricFamilyMap)))
		if err != nil {
			return
		}
		for i := 0; i < len(encoder.metadata.metricFamilyMap); i++ {
			metricFamilyMetadata := encoder.metadata.metricFamilyMap[uint64(i)]
			_, err = writeRawInt(w, uint64(metricFamilyMetadata.metricType))
			if err != nil {
				return
			}
			_, err = writeRawInt(w, uint64(len(metricFamilyMetadata.name)))
			if err != nil {
				return
			}
			_, err = w.WriteString(metricFamilyMetadata.name)
			if err != nil {
				return
			}
			_, err = writeRawInt(w, uint64(len(metricFamilyMetadata.help)))
			if err != nil {
				return
			}
			_, err = w.WriteString(metricFamilyMetadata.help)
			if err != nil {
				return
			}
			for j := 0; j < len(metricFamilyMetadata.labelMap); j++ {
				_, err = writeRawInt(w, uint64(len(metricFamilyMetadata.labelMap[uint64(j)])))
				if err != nil {
					return
				}
				_, err = w.WriteString(metricFamilyMetadata.labelMap[uint64(j)])
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
func writeRawInt(w enhancedWriter, i uint64) (int, error) {
	err := binary.Write(w, binary.LittleEndian, i)
	if err != nil {
		return 0, err
	}
	return int(reflect.TypeOf(i).Size()), err
}
