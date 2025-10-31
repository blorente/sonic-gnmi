package transformer

import (
	"reflect"
	"strconv"

	"github.com/Azure/sonic-mgmt-common/translib/db"
	lvl "github.com/Azure/sonic-mgmt-common/translib/log"
	"github.com/Azure/sonic-mgmt-common/translib/ocbinds"
	"github.com/Azure/sonic-mgmt-common/translib/tlerr"
	log "github.com/golang/glog"
)

type fieldInt32LeafPair struct {
	field string
	leaf  **int32
}

type fieldUint32LeafPair struct {
	field string
	leaf  **uint32
}

type fieldInt64LeafPair struct {
	field string
	leaf  **int64
}

type fieldFloat64LeafPair struct {
	field string
	leaf  **float64
}

type fieldU64LeafPair struct {
	field string
	leaf  **uint64
}
type fieldBinaryLeafPair struct {
	field string
	leaf  *ocbinds.Binary
}

type pairTypes interface {
	fieldInt32LeafPair | fieldUint32LeafPair | fieldInt64LeafPair | fieldU64LeafPair | fieldFloat64LeafPair
}

func processFieldLeafPairs[T pairTypes](entry *db.Value, fls []T) error {
	for _, fl := range fls {
		fieldValue := reflect.ValueOf(fl)
		switch any(fl).(type) {
		case fieldUint32LeafPair:
			attr := fieldValue.Interface().(fieldUint32LeafPair)
			fieldValue, ok := entry.Field[attr.field]
			if !ok {
				log.V(lvl.DEBUG).Infof("Attr %v missing", attr)
				continue
			}
			value, err := strconv.ParseUint(fieldValue, 10, 32)
			if err != nil {
				return err
			}
			valueUint32 := uint32(value)
			*attr.leaf = &valueUint32
		case fieldInt64LeafPair:
			attr := fieldValue.Interface().(fieldInt64LeafPair)
			fieldValue, ok := entry.Field[attr.field]
			if !ok {
				log.V(lvl.DEBUG).Infof("Attr %v missing", attr.field)
				continue
			}
			value, err := strconv.ParseInt(fieldValue, 10, 64)
			if err != nil {
				return err
			}
			*attr.leaf = &value
		case fieldU64LeafPair:
			attr := fieldValue.Interface().(fieldU64LeafPair)
			fieldValue, ok := entry.Field[attr.field]
			if !ok {
				log.V(lvl.DEBUG).Infof("Attr %v missing", attr)
				continue
			}
			value, err := strconv.ParseUint(fieldValue, 10, 64)
			if err != nil {
				return err
			}
			*attr.leaf = &value
		case fieldFloat64LeafPair:
			attr := fieldValue.Interface().(fieldFloat64LeafPair)
			fieldValue, ok := entry.Field[attr.field]
			if !ok {
				log.V(lvl.DEBUG).Infof("Attr %v missing", attr)
				continue
			}
			value, err := strconv.ParseFloat(fieldValue, 64)
			if err != nil {
				return err
			}
			value64 := float64(value)
			*attr.leaf = &value64
		default:
			log.V(lvl.ERROR).Infof("Unknown  field type during parse")

		}
	}
	return nil
}

func readAndParseCounters(entry *db.Value, fls []fieldU64LeafPair) error {
	for _, fl := range fls {
		if e := readAndParseCounter(entry, fl.field, fl.leaf); e != nil {
			switch e.(type) {
			case tlerr.NotFoundError:
				continue
			}
			return e
		}
	}
	return nil
}

func readAndParseCounter(entry *db.Value, attr string, counter_val **uint64) error {
	val1, ok := entry.Field[attr]
	if !ok {
		return tlerr.NotFound("Attr " + attr + " missing")
	}
	v, err := strconv.ParseUint(val1, 10, 64)
	if err != nil {
		return err
	}
	*counter_val = &v
	return nil
}

// Extracts an int32 value from the DB entry field.
func extractInt32(fieldName string, dbEntry *db.Value) *int32 {
	redisStr, ok := dbEntry.Field[fieldName]
	if !ok {
		return nil
	}
	intVal, err := strconv.Atoi(redisStr)
	if err != nil {
		log.V(lvl.ERROR).Infof("Unable to convert field %s to int32. Value was %s. Error %w", fieldName, redisStr, err)
		return nil
	}
	int32Val := int32(intVal)
	return &int32Val
}

// Extracts an uint32 value from the DB entry field.
func extractUInt32(fieldName string, dbEntry *db.Value) *uint32 {
	redisStr, ok := dbEntry.Field[fieldName]
	if !ok {
		return nil
	}
	intVal, err := strconv.Atoi(redisStr)
	if err != nil {
		log.V(lvl.ERROR).Infof("Unable to convert field %s to uint32. Value was %s. Error %w", fieldName, redisStr, err)
		return nil
	}
	uint32Val := uint32(intVal)
	return &uint32Val
}

// Extracts an uint64 value from the DB entry field.
func extractUInt64(fieldName string, dbEntry *db.Value) *uint64 {
	redisStr, ok := dbEntry.Field[fieldName]
	if !ok {
		return nil
	}
	intVal, err := strconv.ParseUint(redisStr, 10, 64)
	if err != nil {
		log.V(lvl.ERROR).Infof("Unable to convert field %s to uint64. Value was %s. Error %w", fieldName, redisStr, err)
		return nil
	}
	uint64Val := uint64(intVal)
	return &uint64Val
}

// Extracts a string value from the DB entry field.
func extractString(fieldName string, dbEntry *db.Value) *string {
	redisStr, ok := dbEntry.Field[fieldName]
	if !ok {
		return nil
	}

	return &redisStr
}

// Extracts a float32 string from the DB entry field. Converts this string to a 4 byte binary value compatible with oc:ieeefloat32 format.
func extractFloat32Str(fieldName string, dbEntry *db.Value) ocbinds.Binary {
	redisStr, ok := dbEntry.Field[fieldName]
	if !ok {
		return nil
	}
	base64Str, err := float32StrTo4Bytes(redisStr)
	if err != nil {
		log.V(lvl.ERROR).Infof("Unable to convert field %s to float string. Value was %s. Error %w", fieldName, redisStr, err)
		return nil
	}
	return base64Str
}

// Extracts an bool value from the DB entry field.
func extractBool(fieldName string, dbEntry *db.Value) *bool {
	redisStr, ok := dbEntry.Field[fieldName]
	if !ok {
		return nil
	}
	boolVal, err := strconv.ParseBool(redisStr)
	if err != nil {
		log.V(lvl.ERROR).Infof("Unable to convert field %s to bool. Value was %s. Error %w", fieldName, redisStr, err)
		return nil
	}
	return &boolVal
}
