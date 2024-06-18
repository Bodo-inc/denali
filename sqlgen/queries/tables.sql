-- name: ListTables :many
SELECT name FROM objects WHERE type = 'TABLE' AND namespace_pk = sqlc.arg('nsPk');

-- name: GetTableHelper :one
SELECT pk, last_sequence_number, metadata_location FROM objects
WHERE type = 'TABLE' AND name = sqlc.arg('name') AND namespace_pk = sqlc.arg('nsPk');

-- name: RegisterTable :exec
INSERT INTO objects (name, type, namespace_pk, metadata_location, last_sequence_number) 
    VALUES (sqlc.arg('name'), 'TABLE', sqlc.arg('nsPk'), sqlc.arg('metadataLoc'), sqlc.arg('lastSeq'));

-- name: UpdateTable :execrows
UPDATE objects 
    SET metadata_location = sqlc.arg('metadataLoc'), last_sequence_number = last_sequence_number + 1
    WHERE pk = sqlc.arg('pk') AND last_sequence_number = sqlc.arg('lastSeq') AND type = 'TABLE';

-- name: RenameTable :execrows
UPDATE objects SET name = sqlc.arg('name') WHERE pk = sqlc.arg('pk') AND type = 'TABLE';

-- name: RenameAndMoveTable :execrows
UPDATE objects SET name = sqlc.arg('name'), namespace_pk = sqlc.arg('nsPk')
    WHERE pk = sqlc.arg('pk') AND type = 'TABLE';

-- name: DropTable :execrows
DELETE FROM objects WHERE pk = sqlc.arg('pk') AND type = 'TABLE';
