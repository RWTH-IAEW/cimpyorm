def test_parse_meta(acquire_db, dummy_source):
    _, session = acquire_db
    assert dummy_source.tree
    assert dummy_source.nsmap == {'cim': 'http://iec.ch/TC57/2013/CIM-schema-cim16#',
                                  'entsoe': 'http://entsoe.eu/CIM/SchemaExtension/3/1#',
                                  'md': 'http://iec.ch/TC57/61970-552/ModelDescription/1#',
                                  'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'}
    assert dummy_source.cim_version == "16"
