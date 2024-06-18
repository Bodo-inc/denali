-- Note: IS NOT DISTINCT FROM is a null-safe equality operator
-- Equivalent to (col = val OR (col IS NULL AND val IS NULL))

-- Get Namespace PK from Path
-- name: GetNamespacePKHelper :one
SELECT pk FROM namespaces WHERE parent_path = sqlc.arg('parentPath') AND name = sqlc.arg('name');

-- Get Child Namespace Names
-- name: GetChildNamespaceNames :many
SELECT name FROM namespaces WHERE parent_path = sqlc.arg('nsPath');

-- name: GetNamespaceProperties :many
SELECT key, value FROM namespace_properties WHERE namespace_pk = sqlc.arg('nsPk');

-- name: CreateNamespace :one
INSERT INTO namespaces (name, parent_pk, parent_path) VALUES (sqlc.arg('name'), sqlc.arg('parentPk'), sqlc.arg('parentPath')) RETURNING pk;
-- name: CreateNamespaceProperty :exec
INSERT INTO namespace_properties (namespace_pk, key, value) VALUES (sqlc.arg('nsPk'), sqlc.arg('key'), sqlc.arg('value'));

-- Update Namespace Property by Key
-- name: UpdateNamespaceProperty :execrows
INSERT INTO namespace_properties (namespace_pk, key, value)
VALUES (sqlc.arg('nsPk'), sqlc.arg('key'), sqlc.arg('value'))
ON CONFLICT (namespace_pk, key)
DO UPDATE SET value = sqlc.arg('value');

-- Delete Namespace Property by Key
-- name: DeleteNamespaceProperty :execrows
DELETE FROM namespace_properties WHERE namespace_pk = sqlc.arg('nsPk') AND key = sqlc.arg('key');

-- Delete Namespace (Cascade Deletes Properties)
-- name: DropNamespace :execrows
DELETE FROM namespaces WHERE name = sqlc.arg('name') AND parent_path = sqlc.arg('parentPath');
