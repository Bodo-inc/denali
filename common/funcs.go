package common

import (
	"fmt"
	"strings"

	icebergTable "github.com/apache/iceberg-go/table"
)

func SliceMap[T any, U any](f func(T) U, s []T) []U {
	var result []U
	for _, v := range s {
		result = append(result, f(v))
	}
	return result
}

type NamespaceID []string

func NamespaceIDFromPath(path string) NamespaceID {
	if path == "" {
		return nil
	}

	return strings.Split(path, "\x1F")
}

func (n NamespaceID) Strings() string {
	wrapped := SliceMap(func(s string) string { return fmt.Sprintf("'%v'", s) }, n)
	return strings.Join(wrapped, ".")
}

func (n NamespaceID) ParentChild() (NamespaceID, string) {
	if len(n) == 0 {
		return nil, ""
	}

	return n[:len(n)-1], n[len(n)-1]
}

type TableIdentifier struct {
	Namespace NamespaceID `json:"namespace"`
	Name      string      `json:"name"`
}

func (TableIdentifier) FromIcebergID(id icebergTable.Identifier) TableIdentifier {
	return TableIdentifier{
		Namespace: id[:len(id)-1],
		Name:      id[len(id)-1],
	}
}

func (id TableIdentifier) ToIcebergID() icebergTable.Identifier {
	return append(id.Namespace, id.Name)
}

func (id TableIdentifier) Strings() string {
	return strings.Join(id.ToIcebergID(), ".")
}
