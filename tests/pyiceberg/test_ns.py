def test_default_namespace(catalog):
    assert catalog.list_namespaces() == [("default",)]


def test_create_drop_namespace(catalog):
    assert catalog.list_namespaces() == [("default",)]
    catalog.create_namespace("test")
    assert catalog.list_namespaces() == [("default",), ("test",)]
    assert catalog.load_namespace_properties("test") == {}
    catalog.drop_namespace("test")
    assert catalog.list_namespaces() == [("default",)]


def test_create_drop_namespace_with_properties(catalog):
    assert catalog.list_namespaces() == [("default",)]
    props = { "creator": "denali" }
    catalog.create_namespace("test", props)
    assert catalog.list_namespaces() == [("default",), ("test",)]
    assert catalog.load_namespace_properties("test") == props
    catalog.drop_namespace("test")
    assert catalog.list_namespaces() == [("default",)]


def test_create_sub_namespace(catalog):
    assert catalog.list_namespaces("default") == []
    props = { "owner": "pyiceberg" }
    catalog.create_namespace("default.def_inner", props)
    
    # Note: Bug in PyIceberg, does not follow the REST spec
    assert catalog.list_namespaces("default") == [("default", "default", "def_inner")]
    assert catalog.load_namespace_properties("default.def_inner") == props

    # Attempt to delete `default` should fail because of sub-namespace
    # TODO: Change error thrown from NoSuchNamespaceError to another
    # with pytest.raises(NoSuchNamespaceError):
    #     catalog.drop_namespace("default.def_inner")

    # Cleanup
    catalog.drop_namespace("default.def_inner")
    assert catalog.list_namespaces("default") == []
