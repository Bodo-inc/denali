package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"

	"github.com/Bodo-inc/denali/common"
	"github.com/Bodo-inc/denali/logic"
)

type CustomRestError struct {
	Code    int    `json:"code"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

func (e CustomRestError) Error() string {
	return e.Message
}

func (e CustomRestError) HttpCode() int {
	return e.Code
}

func BadRequestError(msg string) *CustomRestError {
	return &CustomRestError{
		Code:    http.StatusBadRequest,
		Type:    "BadRequestError",
		Message: msg,
	}
}

func InvalidJsonBodyError(op string) *CustomRestError {
	return BadRequestError("Malformed Request: Invalid JSON Body for operation `" + op + "`")
}

func NotImplementedError() *CustomRestError {
	return &CustomRestError{
		Code:    http.StatusNotImplemented,
		Type:    "NotImplementedError",
		Message: "Not implemented",
	}
}

func mapError(err error) *CustomRestError {
	if err == nil {
		return nil
	}

	if customErr, ok := err.(CustomRestError); ok {
		return &customErr
	}

	msg := err.Error()
	name := reflect.TypeOf(err).Name()

	if restErr, ok := err.(common.RestError); ok {
		return &CustomRestError{
			Code:    restErr.HttpCode(),
			Type:    name,
			Message: msg,
		}
	} else {
		return &CustomRestError{
			Code:    http.StatusInternalServerError,
			Type:    "InternalError",
			Message: msg,
		}
	}
}

type Router struct {
	State *logic.State
	Mux   *http.ServeMux
}

func (r *Router) Config() *logic.Config {
	return r.State.Config
}

// Error function to panic if error is not nil
// Useful for quick error handling of functions that return a value and error
func errPanic[O any](x O, err error) O {
	if err != nil {
		panic(err)
	}
	return x
}

func HandleAPI[O any](r *Router, path string, handler func(ctx context.Context, req *http.Request) (res *O, err error)) {
	r.Mux.HandleFunc(path, func(w http.ResponseWriter, req *http.Request) {
		res, err := handler(req.Context(), req)
		w.Header().Set("Content-Type", "application/json")

		if res == nil && err == nil {
			// If both are nil, return 204 for successful and no content
			w.WriteHeader(http.StatusNoContent)

		} else if err != nil {
			// If error is not nil, extract and set the error code
			// and marshal the error to JSON
			var errFormat struct {
				Error *CustomRestError `json:"error"`
			}
			errFormat.Error = mapError(err)

			w.WriteHeader(errFormat.Error.Code)
			out := errPanic(json.Marshal(errFormat))
			w.Write(out)

		} else {
			// If response is not nil, marshal the response to JSON
			w.WriteHeader(http.StatusOK)
			out := errPanic(json.Marshal(res))
			w.Write(out)
		}
	})
}
