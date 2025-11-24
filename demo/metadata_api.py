def get_resource_metadata_field(catalog_url, resource_id, field_uri):
    """
    Fetches the value of a specific metadata field for a resource from an RDF catalog.

    Args:
        catalog_url (str): The base URL of the catalog (e.g. "https://data.zg.ch/store/1").
        resource_id (str): The identifier of the resource (e.g. "1461").
        field_uri (str): The metadata field to fetch. Can be a full URI or a prefixed name (e.g. "dcat:accessURL").

    Returns:
        str or None: The value of the field as a string, or None if not found.

    Notes:
        - Supports both full URIs and prefixed names for common vocabularies (dcat, dct, schema, foaf, rdfs).
        - Loads the RDF metadata for the resource and extracts the first value for the given field.
        - Raises ValueError if an unknown namespace prefix is provided.
    """
    from rdflib import Graph, URIRef, Namespace

    # Namespace mapping
    namespaces = {
        "dcat": "http://www.w3.org/ns/dcat#",
        "dct": "http://purl.org/dc/terms/",
        "dcterms": "http://purl.org/dc/terms/",
        "schema": "http://schema.org/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    }

    # Resolve field_uri
    if ":" in field_uri and not field_uri.startswith("http"):
        prefix, local = field_uri.split(":", 1)
        ns_uri = namespaces.get(prefix)
        if ns_uri:
            pred = Namespace(ns_uri)[local]
        else:
            raise ValueError(f"Unknown namespace prefix: {prefix}")
    else:
        pred = URIRef(field_uri)

    g = Graph()
    resource_url = f"{catalog_url}/metadata/{resource_id}"
    g.parse(resource_url)
    resource_ref = URIRef(f"{catalog_url}/resource/{resource_id}")
    for o in g.objects(resource_ref, pred):
        return str(o)
    return None
