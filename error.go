package inmem

import "errors"

var ErrKeyNotExist = errors.New("Selected key has not been set")
var ErrParseOperation = errors.New("Error parse operation")
var ErrSetEmpty = errors.New("Selected set already empty")
