package common

import (
	"fmt"
	"net/http"
)

type RestError interface {
	HttpCode() int
}

type NamespaceNotFoundError struct {
	Path NamespaceID
}

func (e NamespaceNotFoundError) Error() string {
	return fmt.Sprintf("Namespace `%v` does not exist", e.Path)
}

func (e NamespaceNotFoundError) HttpCode() int {
	return http.StatusNotFound
}

type TableNotFoundError struct {
	Id TableIdentifier
}

func (e TableNotFoundError) Error() string {
	return fmt.Sprintf("Table %v does not exist in namespace %v", e.Id.Name, e.Id.Namespace)
}

func (e TableNotFoundError) HttpCode() int {
	return http.StatusNotFound
}

type ViewNotFoundError struct {
	Id TableIdentifier
}

func (e ViewNotFoundError) Error() string {
	return fmt.Sprintf("View %v does not exist in namespace %v", e.Id.Name, e.Id.Namespace)
}

func (e ViewNotFoundError) HttpCode() int {
	return http.StatusNotFound
}
