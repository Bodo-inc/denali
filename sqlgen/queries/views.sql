-- name: ListViews :many
SELECT name FROM objects WHERE type = 'VIEW' AND namespace_pk = sqlc.arg('nsPk');

-- name: GetViewHelper :one
SELECT pk, metadata_location FROM objects
    WHERE type = 'VIEW' AND name = sqlc.arg('name') AND namespace_pk = sqlc.arg('nsPk');

-- name: CreateView :one
INSERT INTO objects (name, type, namespace_pk, metadata_location) 
    VALUES (sqlc.arg('name'), 'VIEW', sqlc.arg('nsPk'), sqlc.arg('metadataLoc')) RETURNING pk;

-- name: UpdateView :execrows
UPDATE objects SET metadata_location = sqlc.arg('metadataLoc')
    WHERE pk = sqlc.arg('pk') AND type = 'VIEW';

-- name: RenameView :execrows
UPDATE objects SET name = sqlc.arg('name')
    WHERE pk = sqlc.arg('pk') AND type = 'VIEW';

-- name: RenameAndMoveView :execrows
UPDATE objects SET name = sqlc.arg('name'), namespace_pk = sqlc.arg('nsPk')
    WHERE pk = sqlc.arg('pk') AND type = 'VIEW';

-- name: DropView :execrows
DELETE FROM objects WHERE pk = sqlc.arg('pk') AND type = 'VIEW';
