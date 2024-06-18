-- +goose Up
-- Namespace Elements
CREATE TABLE namespaces (
    pk INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR NOT NULL,
    parent_pk BIGINT,
    -- Cached Path of Parent's Hierarchy for Faster Lookups
    -- Avoided Postgres LTree for Portability
    parent_path VARCHAR NOT NULL,
    -- Unique Namespace Name per Parent
    CONSTRAINT ns_uq_name_pk UNIQUE (name, parent_pk),
    -- Unique Path per Namespace
    CONSTRAINT ns_uq_name_path UNIQUE (name, parent_path),
    -- Foreign Key to Parent PK for Hierarchical Namespaces
    -- Deleting Non-Empty Namespaces should fail
    CONSTRAINT ns_fk_parent FOREIGN KEY (parent_pk)
    REFERENCES namespaces(pk) ON DELETE RESTRICT
);

-- Default Namespace
-- Table and View Objects must be in a Namespace
INSERT INTO namespaces (name, parent_path) VALUES ('default', '');

-- Key-Value Properties for Namespaces
CREATE TABLE namespace_properties (
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    namespace_pk BIGINT NOT NULL,
    -- Primary Keys per Namespace
    PRIMARY KEY (namespace_pk, key),
    -- Foreign Key to Namespace PK, Auto-Delete Properties when Namespace is Deleted
    CONSTRAINT nsprop_fk_namespace FOREIGN KEY (namespace_pk) 
    REFERENCES namespaces(pk) ON DELETE CASCADE
);

-- Iceberg Entities (Tables, Views, etc.) in Namespaces
CREATE TABLE objects (
    pk INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR NOT NULL,
    -- 'TABLE' or 'VIEW' only allowed, no ENUM in SQLite
    type VARCHAR NOT NULL,
    namespace_pk BIGINT NOT NULL,
    last_sequence_number BIGINT NOT NULL DEFAULT 0,
    metadata_location VARCHAR NOT NULL,
    -- Unique Object Name per Namespace
    -- Note, namespaces and objects with the same parent can share names
    -- TODO: May want to revisit this in the future
    CONSTRAINT obj_uq_name_pk UNIQUE (name, namespace_pk),
    CONSTRAINT obj_fk_namespace FOREIGN KEY (namespace_pk)
    REFERENCES namespaces(pk) ON DELETE RESTRICT
);


-- +goose Down
DROP TABLE objects;
DROP TABLE namespace_properties;
DROP TABLE namespaces;
